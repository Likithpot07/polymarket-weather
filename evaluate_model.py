"""Scorecard: is the model better than the market, and which trades actually make money?

Read-only against your collector DB except for a small cache table of settled
outcomes (event_outcomes). Run it from the repo folder:

    python evaluate_model.py --max-events 120

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
    with db:
        db.executemany("INSERT OR REPLACE INTO event_outcomes VALUES (?,?,?,?)",
                       [(event_id, m, y, now) for m, y in results.items()])
    return results


def snapshot_near(db: sqlite3.Connection, event_id: str, moment: datetime,
                  max_gap_minutes: int) -> tuple[int, str] | None:
    lower = (moment - timedelta(minutes=max_gap_minutes)).isoformat()
    return db.execute(
        """SELECT id,as_of FROM live_decision_runs
           WHERE event_id=? AND as_of<=? AND as_of>=?
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--max-events", type=int, default=120)
    ap.add_argument("--max-gap-minutes", type=int, default=45)
    ap.add_argument("--offline", action="store_true", help="Only use cached/paper outcomes")
    ap.add_argument("--out", default="data/eval_snapshots.csv")
    args = ap.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_path.parent / db_path
    db = sqlite3.connect(db_path, timeout=30)
    db.execute("PRAGMA busy_timeout=10000")
    db.executescript(OUTCOME_DDL)
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
