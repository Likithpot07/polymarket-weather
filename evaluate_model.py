"""Scorecard: is the model better than the market, and which trades actually make money?

Read-only against your collector DB except for a small cache table of settled
outcomes (event_outcomes). Run it from the repo folder:

    python evaluate_model.py --max-events 120
    python evaluate_model.py --lock-speed       # how cheap were locked NOs when we first saw the lock?
    python evaluate_model.py --run-speed        # does the market lag behind new forecast runs?

Paste the printed report back to Claude. It also writes data/eval_snapshots.csv.

What it does
  1. Finds finished events that live.py evaluated, and gets each bracket's
     settled YES/NO result from Polymarket (cached after the first run).
  2. At fixed checkpoints (day before at noon, and target day at 09/12/15/18
     local) it takes:
       - v3 probability  = what live.py stored at that moment
       - v4 probability  = the fixed model, replayed with data known at that time
       - market          = mid price of each bracket at that moment
  3. Scores all three (log loss / Brier: lower is better), tries blends of
     model + market, and simulates simple NO strategies at the real asks.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import date, datetime, time, timedelta, timezone
import json
import math
import re
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from probability import evaluate, parse_time

OUTCOME_DDL = """
CREATE TABLE IF NOT EXISTS event_outcomes (
    event_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    yes_result INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY(event_id, market_id)
);
"""
CHECKPOINTS = (("D-3 12:00", -3, 12), ("D-2 12:00", -2, 12), ("D-1 12:00", -1, 12),
               ("D-1 18:00", -1, 18), ("D 09:00", 0, 9), ("D 12:00", 0, 12),
               ("D 15:00", 0, 15), ("D 18:00", 0, 18))
FEE_RATE = 0.05
EPS = 1e-4


def outcomes_for(db: sqlite3.Connection, event_id: str, offline: bool) -> dict[str, int]:
    cached = dict(db.execute(
        "SELECT market_id,yes_result FROM event_outcomes WHERE event_id=?", (event_id,)))
    if cached:
        return {str(k): int(v) for k, v in cached.items()}
    known = {str(m): int(y) for m, y in db.execute(
        """SELECT market_id,yes_result FROM paper_orders
           WHERE event_id=? AND yes_result IS NOT NULL""", (event_id,))}
    if offline:
        return {}
    from collector import GAMMA, get_json
    from paper import settled_market_results
    try:
        event = get_json(f"{GAMMA}/events/{event_id}")
    except Exception as exc:  # network problems: skip this event
        print(f"  outcome fetch failed for {event_id}: {exc}")
        return {}
    results = settled_market_results(event) if isinstance(event, dict) else {}
    if not results:
        return {}
    for market_id, value in known.items():
        if market_id in results and results[market_id] != value:
            print(f"  WARNING outcome mismatch event {event_id} market {market_id}")
    now = datetime.now(timezone.utc).isoformat()
    try:  # cache only; if the bot is busy writing, skip caching instead of crashing
        with db:
            db.executemany("INSERT OR REPLACE INTO event_outcomes VALUES (?,?,?,?)",
                           [(event_id, m, y, now) for m, y in results.items()])
    except sqlite3.OperationalError as exc:
        print(f"  (outcome cache skipped for {event_id}: {exc})")
    return results


def snapshot_near(db: sqlite3.Connection, event_id: str, moment: datetime,
                  max_gap_minutes: int) -> tuple[int, str] | None:
    lower = (moment - timedelta(minutes=max_gap_minutes)).isoformat()
    return db.execute(
        """SELECT id,as_of FROM live_decision_runs r
           WHERE event_id=? AND as_of<=? AND as_of>=?
             AND EXISTS (SELECT 1 FROM live_model_diagnostics d WHERE d.run_id=r.id)
           ORDER BY as_of DESC LIMIT 1""",
        (event_id, moment.isoformat(), lower),
    ).fetchone()


def mid(bid, ask):
    if bid is None or ask is None:
        return None
    bid, ask = float(bid), float(ask)
    if not 0 <= bid < ask <= 1 or ask - bid > 0.2:
        return None
    return (bid + ask) / 2


def market_probs(rows: dict[str, dict]) -> dict[str, float] | None:
    probs = {}
    for market_id, sides in rows.items():
        yes, no = sides.get("YES", {}), sides.get("NO", {})
        value = mid(yes.get("best_bid"), yes.get("best_ask"))
        if value is None:
            no_mid = mid(no.get("best_bid"), no.get("best_ask"))
            value = None if no_mid is None else 1 - no_mid
        if value is None:
            return None
        probs[market_id] = min(1 - EPS, max(EPS, value))
    total = sum(probs.values())
    if not 0.5 <= total <= 1.5:
        return None
    return {k: v / total for k, v in probs.items()}


def log_loss(probs: dict[str, float], outcome: dict[str, int]) -> float:
    winner = next(m for m, y in outcome.items() if y == 1)
    return -math.log(max(EPS, probs.get(winner, EPS)))


def brier(probs: dict[str, float], outcome: dict[str, int]) -> float:
    return sum((probs.get(m, 0.0) - y) ** 2 for m, y in outcome.items())


def blend(a: dict[str, float], b: dict[str, float], w: float) -> dict[str, float]:
    return {k: w * a[k] + (1 - w) * b[k] for k in a}


def fmt(values: list[float]) -> str:
    return f"{sum(values) / len(values):.4f}" if values else "   n/a"


def run_speed(db: sqlite3.Connection, days: int, models: list[str], jump_c: float = 1.0) -> None:
    """When a weather model's forecast for a day 1-3 days out jumps by >= jump_c,
    does the market's implied temperature move BEFORE our bot downloaded the new run
    (market faster: no edge from speed) or AFTER (market slow: a fast bot could win)?
    Market implied temperature = probability-weighted bracket middle, from live.py's
    saved NO prices (last ~7 days)."""
    from zoneinfo import ZoneInfo
    from probability import bracket as parse_bracket, parse_time

    now = datetime.now(timezone.utc)
    lo_day = (now.date() - timedelta(days=days)).isoformat()
    events = db.execute(
        """SELECT DISTINCT m.event_id,m.city,m.target_date,l.timezone FROM markets m
           JOIN locations l ON l.city=m.city WHERE m.target_date>=? ORDER BY m.target_date""",
        (lo_day,)).fetchall()

    def implied_mean(event_id, centers, moment):
        run = db.execute(
            """SELECT r.id FROM live_decision_runs r WHERE r.event_id=? AND r.as_of<=?
                 AND EXISTS (SELECT 1 FROM live_model_diagnostics d WHERE d.run_id=r.id)
               ORDER BY r.as_of DESC LIMIT 1""", (event_id, moment.isoformat())).fetchone()
        if not run:
            return None
        weights = total = 0.0
        for market_id, bid, ask in db.execute(
                "SELECT market_id,best_bid,best_ask FROM live_decisions WHERE run_id=? AND side='NO'", (run[0],)):
            if market_id not in centers or bid is None or ask is None:
                continue
            p_yes = 1 - (float(bid) + float(ask)) / 2
            weights += max(p_yes, 0.0)
            total += max(p_yes, 0.0) * centers[market_id]
        return total / weights if weights > 0.5 else None

    jumps = []
    for event_id, city, target, tz_name in events:
        tz = ZoneInfo(tz_name)
        centers, unit = {}, None
        for market_id, question in db.execute("SELECT id,question FROM markets WHERE event_id=?", (event_id,)):
            parsed = parse_bracket(question or "")
            if not parsed:
                continue
            lo, hi, unit = parsed
            centers[str(market_id)] = ((lo + hi) / 2 if lo is not None and hi is not None
                                       else (hi - 1 if lo is None else lo + 1))
        if len(centers) < 3:
            continue
        to_unit = (lambda c: c) if unit == "C" else (lambda c: c * 9 / 5 + 32)
        last_hour = None
        for model in models:
            prev = None
            for fetched, value in db.execute(
                    """SELECT b.fetched_at,v.temperature_c FROM forecast_batches b
                       JOIN forecast_values v ON v.batch_id=b.id
                       WHERE b.city=? AND b.model=? AND v.period='daily_max' AND v.valid_time=?
                       ORDER BY b.fetched_at""", (city, model, target)):
                moment = parse_time(str(fetched))
                lead = (date.fromisoformat(target) - moment.astimezone(tz).date()).days
                if prev is not None and 1 <= lead <= 3 and abs(float(value) - prev) >= jump_c:
                    hour = (event_id, moment.replace(minute=0, second=0, microsecond=0))
                    if hour != last_hour and moment <= now - timedelta(hours=3):
                        last_hour = hour
                        direction = 1 if float(value) > prev else -1
                        marks = {k: implied_mean(event_id, centers, moment + timedelta(minutes=k))
                                 for k in (-120, -30, 0, 30, 60, 180)}
                        if all(v is not None for v in marks.values()):
                            jumps.append({"city": city, "target": target, "model": model, "lead": lead,
                                          "when": moment.astimezone(tz).strftime("%m-%d %H:%M"),
                                          "jump": to_unit(float(value)) - to_unit(prev), "dir": direction,
                                          "unit": unit, **marks})
                prev = float(value)

    print(f"=== RUN SPEED: forecast jumps >= {jump_c}C for days 1-3 ahead, last {days} days ===")
    print(f"events checked: {len(events)} | forecast jumps with market prices around them: {len(jumps)}")
    if not jumps:
        print("Not enough saved prices around forecast jumps yet (live.py keeps ~7 days).")
        return
    print("\nMarket's implied temperature around each jump (0 = when our bot downloaded the new run)")
    print(f"{'city':<13}{'target':<11}{'model':<14}{'jump':>6}  {'-2h':>6} {'-30m':>6} {'0':>6} {'+30m':>6} {'+1h':>6} {'+3h':>6}")
    for j in sorted(jumps, key=lambda x: -abs(x["jump"]))[:25]:
        print(f"{j['city'][:12]:<13}{j['target']:<11}{j['model'][:13]:<14}{j['jump']:>+5.1f}{j['unit']} "
              + " ".join(f"{j[k]:>6.1f}" for k in (-120, -30, 0, 30, 60, 180)))
    def moved(j, a, b):  # market move in the direction of the forecast jump
        return (j[b] - j[a]) * j["dir"]
    n = len(jumps)
    before = sum(moved(j, -120, 0) for j in jumps) / n
    after1 = sum(moved(j, 0, 60) for j in jumps) / n
    after3 = sum(moved(j, 0, 180) for j in jumps) / n
    print(f"\nAverage market move in the jump's direction ({jumps[0]['unit']} mix; positive = same way as forecast):")
    print(f"  2h before -> our download : {before:+.2f}")
    print(f"  our download -> +1h       : {after1:+.2f}")
    print(f"  our download -> +3h       : {after3:+.2f}")
    b, a = max(before, 0.0), max(after3, 0.0)
    share = b / (b + a) if (b + a) > 0 else None
    if share is not None:
        print(f"  share of the move that happened BEFORE our download: {share * 100:.0f}%")
    print("\nHow to read: if most of the move is BEFORE our download, the market is faster than us"
          "\nand speed alone won't help. A big move AFTER our download = a fast bot could catch it.")


def lock_speed(db: sqlite3.Connection, days: int) -> None:
    """For brackets the airport readings ruled out on the target day ("locked NO"):
    what did the NO cost just before, at the moment our bot could first know, and
    10/30/60 minutes later? Cheap prices at 'seen' = an opportunity speed could catch.
    Uses the same lock rule as the live bot (2nd-highest reading >= top of bracket
    + 0.5 + margin). Prices come from live.py's saved decisions (last ~7 days).
    """
    from zoneinfo import ZoneInfo
    from probability import LOCK_MARGIN_C, bracket as parse_bracket, parse_time

    since = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    today = datetime.now(timezone.utc).date().isoformat()
    events = db.execute(
        """SELECT DISTINCT m.event_id,m.city,m.target_date,l.timezone FROM markets m
           JOIN locations l ON l.city=m.city
           WHERE m.target_date>=? AND m.target_date<? ORDER BY m.target_date""",
        (since, today)).fetchall()

    def price_at(event_id, market_id, moment, before):
        op, order = ("<=", "DESC") if before else (">=", "ASC")
        row = db.execute(
            f"""SELECT r.as_of,d.best_ask,d.best_bid FROM live_decision_runs r
                JOIN live_decisions d ON d.run_id=r.id
                WHERE r.event_id=? AND r.as_of{op}? AND d.market_id=? AND d.side='NO'
                  AND d.best_ask IS NOT NULL
                ORDER BY r.as_of {order} LIMIT 1""",
            (event_id, moment.isoformat(), market_id)).fetchone()
        return row

    locks = []
    for event_id, city, target, tz_name in events:
        tz = ZoneInfo(tz_name)
        brackets = db.execute(
            """SELECT m.id,m.question,r.station,r.unit FROM markets m
               LEFT JOIN rule_reviews r ON r.market_id=m.id WHERE m.event_id=?""", (event_id,)).fetchall()
        station = next((b[2] for b in brackets if b[2]), None)
        unit = next((b[3] for b in brackets if b[3]), None)
        if not station or unit not in ("C", "F"):
            continue
        readings = []  # (fetched_at, observed_at, value in market unit), target local day only
        seen_keys = set()
        for observed, temp, stored_unit, fetched, rec in db.execute(
                """SELECT o.observed_at,o.temperature,o.unit,o.fetched_at,o.source_record_id
                   FROM observations o JOIN markets m ON o.market_id=m.id
                   WHERE m.event_id=? AND o.station=? ORDER BY o.fetched_at""", (event_id, station)):
            try:
                moment = parse_time(str(observed))
                if moment.astimezone(tz).date().isoformat() != target:
                    continue
                key = str(rec or moment.isoformat())
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                c = float(temp) if stored_unit == "C" else (float(temp) - 32) * 5 / 9
                readings.append((parse_time(str(fetched)), moment, c if unit == "C" else c * 9 / 5 + 32))
            except (TypeError, ValueError):
                continue
        if len(readings) < 2:
            continue
        margin = LOCK_MARGIN_C if unit == "C" else LOCK_MARGIN_C * 9 / 5
        outcomes = dict(db.execute("SELECT market_id,yes_result FROM event_outcomes WHERE event_id=?", (event_id,)))
        for market_id, question, _, _ in brackets:
            parsed = parse_bracket(question or "")
            if not parsed or parsed[1] is None:
                continue
            threshold = parsed[1] + 0.5 + margin
            top2 = []
            for fetched, observed, value in readings:  # in the order the bot learned them
                top2 = sorted(top2 + [value], reverse=True)[:2]
                if len(top2) == 2 and top2[1] >= threshold:
                    before = price_at(event_id, market_id, observed, True)
                    seen = price_at(event_id, market_id, fetched, False)
                    later = {m: price_at(event_id, market_id, fetched + timedelta(minutes=m), False)
                             for m in (10, 30, 60)}
                    locks.append({
                        "city": city, "target": target, "question": question,
                        "local": fetched.astimezone(tz).strftime("%H:%M"),
                        "lag_min": (fetched - observed).total_seconds() / 60,
                        "before": before[1] if before else None,
                        "seen": seen[1] if seen else None,
                        "seen_delay_min": ((parse_time(seen[0]) - fetched).total_seconds() / 60) if seen else None,
                        **{f"+{m}": (r[1] if r else None) for m, r in later.items()},
                        "resolved_yes": outcomes.get(str(market_id)),
                    })
                    break

    print(f"=== LOCK SPEED: brackets ruled out by airport readings, last {days} days ===")
    print(f"events checked: {len(events)} | locked brackets found: {len(locks)}")
    priced = [l for l in locks if l["seen"] is not None]
    print(f"with a NO price saved right after the lock: {len(priced)}")
    if not priced:
        print("No saved prices around lock times (live.py keeps ~7 days). Try again later.")
        return
    f = lambda x: "  n/a" if x is None else f"{x:.3f}"
    print("\nNO price (ask) per locked bracket. 'seen' = first price after our bot got the reading.")
    print(f"{'city':<14}{'date':<11}{'time':>6}{'lag':>5}  {'before':>6} {'seen':>6} {'+10m':>6} {'+30m':>6} {'+60m':>6}  bracket")
    for l in sorted(priced, key=lambda x: x["seen"])[:40]:
        b = re.search(r"be (.+?)(?: on |\?|$)", l["question"] or "")
        print(f"{l['city'][:13]:<14}{l['target']:<11}{l['local']:>6}{l['lag_min']:>4.0f}m  "
              f"{f(l['before']):>6} {f(l['seen']):>6} {f(l['+10']):>6} {f(l['+30']):>6} {f(l['+60']):>6}  "
              f"{(b.group(1) if b else '')[:22]}{'  <-- RESOLVED YES!' if l['resolved_yes'] == 1 else ''}")
    print("\nHow cheap was the NO at the moment we could first know?")
    for lo, hi in [(0, 0.5), (0.5, 0.8), (0.8, 0.95), (0.95, 0.99), (0.99, 1.01)]:
        g = [l for l in priced if lo <= l["seen"] < hi]
        if g:
            gain = sum((1 - l["seen"]) / l["seen"] for l in g) / len(g) * 100
            print(f"  ask {lo:.2f}-{min(hi, 1):.2f}: {len(g):>3} locks | avg profit if bought: {gain:5.1f}% per bet")
    cheap = [l for l in priced if l["seen"] < 0.9]
    for m in (10, 30, 60):
        still = [l for l in cheap if l[f"+{m}"] is not None and l[f"+{m}"] < 0.9]
        print(f"  of {len(cheap)} locks priced under 0.90 when seen, still under 0.90 after {m} min: {len(still)}")
    lags = sorted(l["lag_min"] for l in priced)
    print(f"\nReading delay (airport time -> our bot had it): median {lags[len(lags)//2]:.0f} min, "
          f"worst {lags[-1]:.0f} min")
    bad = [l for l in locks if l["resolved_yes"] == 1]
    print(f"Safety: {len(bad)} locked brackets resolved YES (must be 0)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--max-events", type=int, default=120)
    ap.add_argument("--max-gap-minutes", type=int, default=45)
    ap.add_argument("--offline", action="store_true", help="Only use cached/paper outcomes")
    ap.add_argument("--out", default="data/eval_snapshots.csv")
    ap.add_argument("--lock-speed", action="store_true",
                    help="Only run the locked-NO price/speed study and exit")
    ap.add_argument("--run-speed", action="store_true",
                    help="Only run the new-forecast-run market speed study and exit")
    ap.add_argument("--days", type=int, default=7, help="Days back for --lock-speed / --run-speed")
    args = ap.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_path.parent / db_path
    db = sqlite3.connect(db_path, timeout=30)
    db.execute("PRAGMA busy_timeout=60000")
    db.executescript(OUTCOME_DDL)
    if args.lock_speed:
        lock_speed(db, args.days)
        return
    if args.run_speed:
        run_speed(db, args.days, cfg.get("weather_models", ["ecmwf_ifs025", "gfs_global", "icon_global"]))
        return
    min_days = int(cfg.get("decision", {}).get("min_calibration_days", 14))

    today = datetime.now(timezone.utc).date()
    events = db.execute(
        """SELECT r.event_id,r.city,r.target_date,l.timezone
           FROM (SELECT DISTINCT event_id,city,target_date FROM live_decision_runs) r
           JOIN locations l ON l.city=r.city
           WHERE r.target_date < ?
           ORDER BY r.target_date DESC""",
        ((today - timedelta(days=1)).isoformat(),),
    ).fetchall()[: args.max_events]
    print(f"Finished events with live snapshots: {len(events)}")

    records = []
    coverage: dict[str, int] = defaultdict(int)
    scored_events = 0
    for index, (event_id, city, target, tz_name) in enumerate(events, 1):
        outcome = outcomes_for(db, str(event_id), args.offline)
        if not outcome or sum(outcome.values()) != 1:
            continue
        tz = ZoneInfo(tz_name)
        scored_events += 1
        for label, day_offset, hour in CHECKPOINTS:
            local = datetime.combine(date.fromisoformat(target) + timedelta(days=day_offset), time(hour), tz)
            moment = local.astimezone(timezone.utc)
            snap = snapshot_near(db, str(event_id), moment, args.max_gap_minutes)
            if not snap:
                continue
            run_id, as_of_text = snap
            coverage[label] += 1
            rows: dict[str, dict] = defaultdict(dict)
            for market_id, side, p, bid, ask in db.execute(
                """SELECT market_id,side,model_probability,best_bid,best_ask
                   FROM live_decisions WHERE run_id=?""", (run_id,)):
                rows[str(market_id)][side] = {"p": p, "best_bid": bid, "best_ask": ask}
            if set(rows) != set(outcome):
                continue
            market = market_probs(rows)
            v3 = {m: float(s.get("YES", {}).get("p") or 0.0) for m, s in rows.items()}
            try:
                state = evaluate(db, str(event_id), parse_time(as_of_text), min_days=min_days, persist=False)
            except (ValueError, sqlite3.Error) as exc:
                print(f"  replay failed {event_id} {label}: {exc}")
                continue
            v4 = {str(p["market_id"]): float(p["p_yes"]) for p in state["probabilities"]}
            locked = {str(p["market_id"]): bool(p.get("locked_no")) for p in state["probabilities"]}
            for market_id in outcome:
                no = rows[market_id].get("NO", {})
                records.append({
                    "event_id": event_id, "city": city, "target_date": target,
                    "checkpoint": label, "as_of": as_of_text, "market_id": market_id,
                    "yes_result": outcome[market_id],
                    "v3_yes": v3.get(market_id), "v4_yes": v4.get(market_id),
                    "market_yes": None if market is None else market[market_id],
                    "no_bid": no.get("best_bid"), "no_ask": no.get("best_ask"),
                    "locked_no": int(locked.get(market_id, False)),
                    "calibrated_models": sum(int(c.get("calibration_days", 0)) >= min_days
                                             for c in state["components"]),
                })
        if index % 20 == 0:
            print(f"  processed {index}/{len(events)} events")

    if not records:
        print("No scorable snapshots. Check that live.py has been running on events that have now settled.")
        return
    out = Path(args.out)
    out = out if out.is_absolute() else config_path.parent / out
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)

    print("\n=== 0. COVERAGE: settled events that had a bot snapshot at each checkpoint ===")
    for label, *_ in CHECKPOINTS:
        print(f"{label:<11}{coverage[label]:>5} of {scored_events}")
    report(records)


def report(records: list[dict]) -> None:
    by_snapshot: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        by_snapshot[(r["event_id"], r["checkpoint"])].append(r)
    print("\n=== 1. ACCURACY: who predicts the winning bracket best? (lower = better) ===")
    print(f"{'checkpoint':<11}{'n':>5}  {'LL v3':>8}{'LL v4':>8}{'LL mkt':>8}   {'Brier v3':>9}{'Brier v4':>9}{'Brier mkt':>10}")
    weights = [0.0, 0.25, 0.5, 0.75, 1.0]
    blend_scores: dict[str, dict[float, list[float]]] = defaultdict(lambda: defaultdict(list))
    for label, *_ in CHECKPOINTS:
        ll = defaultdict(list)
        br = defaultdict(list)
        for (event_id, checkpoint), rows in by_snapshot.items():
            if checkpoint != label or any(r["market_yes"] is None for r in rows):
                continue
            outcome = {r["market_id"]: r["yes_result"] for r in rows}
            v3 = {r["market_id"]: r["v3_yes"] for r in rows}
            v4 = {r["market_id"]: r["v4_yes"] for r in rows}
            mk = {r["market_id"]: r["market_yes"] for r in rows}
            for name, probs in (("v3", v3), ("v4", v4), ("mkt", mk)):
                ll[name].append(log_loss(probs, outcome))
                br[name].append(brier(probs, outcome))
            for w in weights:
                blend_scores[label][w].append(log_loss(blend(v4, mk, w), outcome))
        print(f"{label:<11}{len(ll['v3']):>5}  {fmt(ll['v3']):>8}{fmt(ll['v4']):>8}{fmt(ll['mkt']):>8}   "
              f"{fmt(br['v3']):>9}{fmt(br['v4']):>9}{fmt(br['mkt']):>10}")

    print("\n=== 2. BEST MODEL WEIGHT (log loss of w*v4 + (1-w)*market) ===")
    print(f"{'checkpoint':<11}" + "".join(f"{'w=' + str(w):>9}" for w in weights) + "   best")
    for label, scores in blend_scores.items():
        means = {w: sum(v) / len(v) for w, v in scores.items() if v}
        if not means:
            continue
        best = min(means, key=means.get)
        print(f"{label:<11}" + "".join(f"{means[w]:>9.4f}" for w in weights) + f"   w={best}")

    print("\n=== 3. CALIBRATION of NO probability (v4) -- when we say X%, how often is NO right? ===")
    buckets = [(0.0, 0.5), (0.5, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 0.95), (0.95, 0.99), (0.99, 1.01)]
    for name in ("v3_yes", "v4_yes", "market_yes"):
        parts = []
        for lo, hi in buckets:
            xs = [r for r in records if r[name] is not None and lo <= 1 - r[name] < hi]
            if xs:
                won = sum(1 - r["yes_result"] for r in xs) / len(xs)
                parts.append(f"[{lo:.2f},{min(hi, 1):.2f}) n={len(xs)} NO won {won:.1%}")
        print(f"{name.replace('_yes', ''):>6}: " + " | ".join(parts))

    print("\n=== 4. STRATEGY SIM: buy NO at the real ask, $1 of payout per trade, hold to settlement ===")
    print("(one trade per event+checkpoint per strategy; fee 5%*p*(1-p); no extra slippage)")

    def sim(name, pick, early=None):
        n = wins = 0
        pnl = cost_total = 0.0
        for (event_id, checkpoint), rows in by_snapshot.items():
            if early is not None and checkpoint.startswith("D-") != early:
                continue
            chosen = pick(rows)
            if not chosen:
                continue
            ask = float(chosen["no_ask"])
            cost = ask + FEE_RATE * ask * (1 - ask)
            won = chosen["yes_result"] == 0
            n += 1
            wins += won
            pnl += (1.0 if won else 0.0) - cost
            cost_total += cost
        if n:
            print(f"{name:<52} trades={n:>4}  win={wins / n:6.1%}  pnl=${pnl:+7.2f}  roi={pnl / cost_total:+6.1%}")
        else:
            print(f"{name:<52} trades=   0")

    def tradable(rows):
        return [r for r in rows if r["no_ask"] is not None and 0 < float(r["no_ask"]) < 1]

    def cost(r):
        a = float(r["no_ask"])
        return a + FEE_RATE * a * (1 - a)

    def old_v3(rows):  # old live rule: p_no>=0.7, edge>=6%, then LOWEST ask
        c = [r for r in tradable(rows) if 1 - r["v3_yes"] >= 0.7 and (1 - r["v3_yes"]) - cost(r) - 0.01 >= 0.06
             and (1 - r["v3_yes"]) - float(r["no_ask"]) <= 0.4]
        return min(c, key=lambda r: float(r["no_ask"])) if c else None

    def new_rule(w, min_edge):
        def pick(rows):
            c = []
            for r in tradable(rows):
                if r["locked_no"]:
                    continue
                if r["market_yes"] is None:
                    continue
                p = w * (1 - r["v4_yes"]) + (1 - w) * (1 - r["market_yes"])
                if 1 - r["v4_yes"] >= 0.7 and p - cost(r) - 0.01 >= min_edge:
                    c.append((p, r))
            return max(c, key=lambda x: x[0])[1] if c else None
        return pick

    def locked(rows):
        c = [r for r in tradable(rows) if r["locked_no"] and 1 - cost(r) >= 0.005]
        return max(c, key=lambda r: 1 - cost(r)) if c else None

    def market_favourite_no(rows):
        c = [r for r in tradable(rows) if r["market_yes"] is not None]
        return max(c, key=lambda r: r["market_yes"]) if c else None

    for early, title in ((True, "EARLY (1-3 days before)"), (False, "TARGET DAY")):
        print(f"-- {title} --")
        sim("OLD live rule (v3, lowest NO ask)", old_v3, early)
        sim("Always bet NO on market favourite (reference)", market_favourite_no, early)
        for w in (1.0, 0.5, 0.25):
            for e in (0.03, 0.06):
                sim(f"NEW v4 blend w={w}, edge>={e:.0%}, safest NO", new_rule(w, e), early)
    sim("LOCKED NO (obs already above bracket)", locked)
    lk = [r for r in records if r["locked_no"]]
    if lk:
        bad = [r for r in lk if r["yes_result"] == 1]
        print(f"\nLocked-NO sanity: {len(lk)} locked bracket-snapshots, {len(bad)} resolved YES (must be ~0)")
        for r in bad[:10]:
            print("  LOCK FAILED:", r["event_id"], r["city"], r["target_date"], r["checkpoint"], r["market_id"])


if __name__ == "__main__":
    main()
