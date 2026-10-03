"""Backtest early (1-3 days ahead) betting on PAST data -- no waiting needed.

Part A  (seconds, offline): uses station_residuals from station_history.py to test
        several ways of turning the 3 forecasts into probabilities, and picks the
        most accurate. Every prediction only uses history available at that time.
Part B  (network, cached, resumable): downloads past Polymarket temperature
        markets + their price history, then compares our model with the market
        price 1, 2 and 3 days before, and simulates NO bets.

    python backtest_early.py --offline-only      # Part A only
    python backtest_early.py --days 45           # Part A + Part B (first run is slow)

Paste the printed report back to Claude. Details: data/backtest_early_trades.csv
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import date, datetime, time as dtime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from collector import GAMMA, SLUG_PATTERN, get_json  # also installs the Polymarket DNS fix
from paper import settled_market_results
from probability import bracket, cdf

CLOB = "https://clob.polymarket.com"
LEADS = (1, 2, 3)
FEE_RATE = 0.05
EPS = 1e-4
CACHE_DDL = """
CREATE TABLE IF NOT EXISTS backtest_events (slug TEXT PRIMARY KEY, payload TEXT, fetched_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS backtest_prices (token_id TEXT PRIMARY KEY, payload TEXT NOT NULL, fetched_at TEXT NOT NULL);
"""


# ---------------------------------------------------------------- model variants
def load_residuals(db) -> dict:
    """{(city, lead): {day: {model: (forecast_c, actual_c)}}}"""
    data: dict = defaultdict(lambda: defaultdict(dict))
    for city, model, lead, day, forecast_c, actual_c in db.execute(
            "SELECT city,model,lead_days,day,forecast_c,actual_c FROM station_residuals"):
        data[(city, int(lead))][day][model] = (float(forecast_c), float(actual_c))
    return data


_FIT_CACHE: dict = {}


def _base_fit(series: dict, decision_day: date, window: int, min_days: int, sigma_floor: float):
    key = (id(series), decision_day, window)
    if key in _FIT_CACHE:
        return _FIT_CACHE[key]
    start = (decision_day - timedelta(days=window)).isoformat()
    stop = (decision_day - timedelta(days=1)).isoformat()
    train = [v for d, v in series.items() if start <= d <= stop]
    result = None
    if len(train) >= min_days:
        fits = {}
        for m in sorted({m for v in train for m in v}):
            res = [v[m][1] - v[m][0] for v in train if m in v]
            if len(res) < min_days:
                continue
            bias = sum(res) / len(res)
            sd = math.sqrt(sum((r - bias) ** 2 for r in res) / (len(res) - 1))
            fits[m] = (bias, max(sigma_floor, sd))
        if fits:
            result = (train, fits)
    _FIT_CACHE[key] = result
    return result


def fit_components(series: dict, decision_day: date, window: int, variant: str,
                   inflate: float, min_days: int = 20, sigma_floor: float = 0.5):
    """Model fitted only on days finished before decision_day (no peeking)."""
    base = _base_fit(series, decision_day, window, min_days, sigma_floor)
    if not base:
        return None
    train, fits = base
    if variant == "equal-mix":
        return {"kind": "mix", "fits": fits, "weights": {m: 1 / len(fits) for m in fits}, "inflate": inflate}
    raw = {m: 1 / s ** 2 for m, (_, s) in fits.items()}
    total = sum(raw.values())
    weights = {m: r / total for m, r in raw.items()}
    if variant == "invvar-mix":
        return {"kind": "mix", "fits": fits, "weights": weights, "inflate": inflate}
    # "combined": one bell curve around the weighted, bias-corrected average.
    res = []
    for v in train:
        if all(m in v for m in fits):
            combo = sum(weights[m] * (v[m][0] + fits[m][0]) for m in fits)
            res.append(next(iter(v.values()))[1] - combo)
    if len(res) < min_days:
        return None
    bias = sum(res) / len(res)
    sd = max(sigma_floor, math.sqrt(sum((r - bias) ** 2 for r in res) / (len(res) - 1)))
    return {"kind": "combined", "fits": fits, "weights": weights, "bias": bias, "sd": sd, "inflate": inflate}


def dist_prob(model: dict, forecasts: dict, lo_c: float | None, hi_c: float | None) -> float | None:
    """P(lo_c <= daily max < hi_c) in Celsius (bounds already include rounding)."""
    def interval(mean, sd):
        upper = 1.0 if hi_c is None else cdf(hi_c, mean, sd)
        lower = 0.0 if lo_c is None else cdf(lo_c, mean, sd)
        return max(0.0, upper - lower)
    fits = model["fits"]
    if not all(m in forecasts for m in fits):
        return None
    if model["kind"] == "combined":
        mean = sum(model["weights"][m] * (forecasts[m] + fits[m][0]) for m in fits) + model["bias"]
        return interval(mean, model["sd"] * model["inflate"])
    return sum(model["weights"][m] * interval(forecasts[m] + fits[m][0], fits[m][1] * model["inflate"])
               for m in fits)


def part_a(data: dict) -> tuple[str, int, float]:
    print("\n=== PART A: which way of combining forecasts is most accurate? (all cities, past 150 days) ===")
    print("Score = average probability given to the 1°C bracket that actually happened (higher = better)")
    variants = [(v, w, i) for v in ("equal-mix", "invvar-mix", "combined")
                for w in (45, 90) for i in (1.0, 1.2)]
    scores = {}
    for variant, window, inflate in variants:
        per_lead = defaultdict(list)
        for (city, lead), series in data.items():
            if lead not in LEADS:
                continue
            for day, v in series.items():
                decision = date.fromisoformat(day) - timedelta(days=lead)
                model = fit_components(series, decision, window, variant, inflate)
                if not model:
                    continue
                actual = next(iter(v.values()))[1]
                centre = round(actual)
                p = dist_prob(model, {m: f for m, (f, _) in v.items()}, centre - 0.5, centre + 0.5)
                if p is not None:
                    per_lead[lead].append(p)
        scores[(variant, window, inflate)] = per_lead
    print(f"{'method':<12}{'window':>7}{'widen':>7}" + "".join(f"{'D-' + str(l):>8}" for l in LEADS) + f"{'avg LL':>9}")
    ranked = []
    for key, per_lead in scores.items():
        all_p = [p for l in LEADS for p in per_lead[l]]
        if not all_p:
            continue
        ll = sum(-math.log(max(EPS, p)) for p in all_p) / len(all_p)
        ranked.append((ll, key))
        print(f"{key[0]:<12}{key[1]:>7}{key[2]:>7.2f}" +
              "".join(f"{(sum(per_lead[l]) / len(per_lead[l]) if per_lead[l] else 0):>8.3f}" for l in LEADS) +
              f"{ll:>9.4f}")
    ranked.sort()
    best = ranked[0][1]
    print(f"BEST: method={best[0]} window={best[1]} widen={best[2]} (lowest log loss {ranked[0][0]:.4f})")
    return best


# ---------------------------------------------------------------- past markets
WORKERS = 8


def fetch_event(slug: str):
    """Network only (thread-safe). Returns (slug, payload or None, cacheable)."""
    try:
        payload = get_json(f"{GAMMA}/events/slug/{quote(slug, safe='')}", retries=3)
        return slug, payload if isinstance(payload, dict) else None, True
    except HTTPError:
        return slug, None, True          # e.g. 404: market didn't exist that day
    except (URLError, OSError, ValueError):
        return slug, None, False         # network trouble: try again next run


def fetch_prices(token: str, start: datetime, end: datetime):
    history = []
    for fidelity in (60, 720):
        params = {"market": token, "startTs": int(start.timestamp()), "endTs": int(end.timestamp()),
                  "fidelity": fidelity}
        try:
            payload = get_json(f"{CLOB}/prices-history?{urlencode(params)}", retries=3)
        except (HTTPError, URLError, OSError, ValueError):
            return token, [], False
        history = [(int(p["t"]), float(p["p"])) for p in (payload or {}).get("history", [])
                   if "t" in p and "p" in p]
        if history:
            break
    return token, history, True


def get_events(db, pool, slugs: list[str]) -> dict:
    result, missing = {}, []
    for slug in slugs:
        row = db.execute("SELECT payload FROM backtest_events WHERE slug=?", (slug,)).fetchone()
        if row:
            result[slug] = json.loads(row[0]) if row[0] else None
        else:
            missing.append(slug)
    now = datetime.now(timezone.utc).isoformat()
    with db:
        for slug, payload, cacheable in pool.map(fetch_event, missing):
            result[slug] = payload
            if cacheable:
                db.execute("INSERT OR REPLACE INTO backtest_events VALUES (?,?,?)",
                           (slug, json.dumps(payload) if payload else None, now))
    return result


def get_prices(db, pool, tokens: list[tuple[str, datetime, datetime]]) -> dict:
    result, missing = {}, []
    for token, start, end in tokens:
        row = db.execute("SELECT payload FROM backtest_prices WHERE token_id=?", (token,)).fetchone()
        if row:
            result[token] = [tuple(x) for x in json.loads(row[0])]
        else:
            missing.append((token, start, end))
    now = datetime.now(timezone.utc).isoformat()
    with db:
        for token, history, cacheable in pool.map(lambda a: fetch_prices(*a), missing):
            result[token] = history
            if cacheable:
                db.execute("INSERT OR REPLACE INTO backtest_prices VALUES (?,?,?)",
                           (token, json.dumps(history), now))
    return result


def price_at(history, moment: datetime, max_age_hours: float = 6):
    ts = moment.timestamp()
    before = [p for t, p in history if t <= ts and ts - t <= max_age_hours * 3600]
    return before[-1] if before else None


def part_b(db, cache, data, best, days: int, spread: float, out_path: Path):
    from concurrent.futures import ThreadPoolExecutor
    variant, window, inflate = best
    cities = {}
    for slug, city, tz_name in db.execute(
            """SELECT DISTINCT e.slug,m.city,l.timezone FROM events e JOIN markets m ON m.event_id=e.id
               JOIN locations l ON l.city=m.city WHERE e.slug IS NOT NULL"""):
        match = SLUG_PATTERN.match(str(slug))
        if match:
            cities[match.group(1)] = (city, tz_name)
    # Only days we have past forecasts for (station_history.py stops ~2 days before it ran).
    last_forecast_day = max(date.fromisoformat(d) for series in data.values() for d in series)
    end_day = min(date.today() - timedelta(days=2), last_forecast_day)
    days_list = [end_day - timedelta(days=i) for i in range(days)]
    print(f"\n=== PART B: past markets, {len(cities)} cities x {days} days, "
          f"{days_list[-1]} -> {days_list[0]} (cached; safe to stop and re-run) ===", flush=True)
    records = []
    counts = defaultdict(int)
    started = time.monotonic()
    with ThreadPoolExecutor(WORKERS) as pool:
        for day_index, day in enumerate(days_list, 1):
            todo = {}
            for slug_city, (city, tz_name) in sorted(cities.items()):
                if not any(day.isoformat() in (data.get((city, l)) or {}) for l in LEADS):
                    counts["no forecast history for city"] += 1
                    continue
                slug = f"highest-temperature-in-{slug_city}-on-{day.strftime('%B').lower()}-{day.day}-{day.year}"
                todo[slug] = (city, tz_name)
            events = get_events(cache, pool, list(todo))
            usable = []
            for slug, (city, tz_name) in todo.items():
                event = events.get(slug)
                if not event:
                    counts["no market that day"] += 1
                    continue
                outcome = settled_market_results(event)
                markets = [m for m in event.get("markets") or [] if str(m.get("id")) in outcome]
                if not markets or sum(outcome.values()) != 1:
                    counts["not settled"] += 1
                    continue
                brackets = {}
                for m in markets:
                    parsed = bracket(str(m.get("question") or ""))
                    tokens = m.get("clobTokenIds")
                    tokens = json.loads(tokens) if isinstance(tokens, str) else tokens
                    if not parsed or not tokens:
                        break
                    brackets[str(m["id"])] = (parsed, str(tokens[0]))
                if len(brackets) != len(markets):
                    counts["bracket parse failed"] += 1
                    continue
                usable.append((city, tz_name, outcome, brackets))
            wanted = []
            for city, tz_name, outcome, brackets in usable:
                tz = ZoneInfo(tz_name)
                start = datetime.combine(day - timedelta(days=4), dtime(0), tz)
                finish = datetime.combine(day, dtime(12), tz)
                wanted.extend((tok, start, finish) for _, tok in brackets.values())
            prices = get_prices(cache, pool, wanted)
            for city, tz_name, outcome, brackets in usable:
                tz = ZoneInfo(tz_name)
                if not any(prices.get(tok) for _, tok in brackets.values()):
                    counts["no price history"] += 1
                    continue
                scored = False
                for lead in LEADS:
                    series = data.get((city, lead))
                    if not series or day.isoformat() not in series:
                        continue
                    decision = day - timedelta(days=lead)
                    model = fit_components(series, decision, window, variant, inflate)
                    if not model:
                        continue
                    forecasts = {m: f for m, (f, _) in series[day.isoformat()].items()}
                    moment = datetime.combine(decision, dtime(12), tz)
                    snap = []
                    for mid, ((lo, hi, unit), tok) in brackets.items():
                        to_c = (lambda x: x) if unit == "C" else (lambda x: (x - 32) * 5 / 9)
                        lo_c = None if lo is None else to_c(lo - 0.5)
                        hi_c = None if hi is None else to_c(hi + 0.5)
                        p_model = dist_prob(model, forecasts, lo_c, hi_c)
                        p_mkt = price_at(prices.get(tok, []), moment)
                        if p_model is None or p_mkt is None:
                            snap = []
                            break
                        snap.append({"city": city, "day": day.isoformat(), "lead": lead, "market_id": mid,
                                     "bracket": f"{lo}-{hi}{unit}", "yes_result": outcome[mid],
                                     "model_yes": p_model, "market_yes": p_mkt,
                                     "best_sigma_c": model.get("sd", min(s for _, s in model["fits"].values()))})
                    if snap:
                        records.extend(snap)
                        counts[f"scored D-{lead}"] += 1
                        scored = True
                if not scored:
                    counts["no price at checkpoint"] += 1
            mins = (time.monotonic() - started) / 60
            summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            print(f"  day {day_index}/{len(days_list)} ({day}) done in {mins:.1f} min | {summary}", flush=True)
    if not records:
        print("No past markets with prices found. Paste this output to Claude.")
        return
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    report_b(records, spread)


def report_b(records, spread):
    snaps = defaultdict(list)
    for r in records:
        snaps[(r["city"], r["day"], r["lead"])].append(r)
    print("\n--- B1. ACCURACY: model vs market (log loss, lower = better) ---")
    print(f"{'lead':<6}{'events':>7}{'model':>9}{'market':>9}   best blend w (share of model)")
    for lead in LEADS:
        ll = defaultdict(list)
        for (city, day, l), rows in snaps.items():
            if l != lead:
                continue
            total = sum(max(EPS, r["market_yes"]) for r in rows)
            win = next(r for r in rows if r["yes_result"] == 1)
            model_total = sum(r["model_yes"] for r in rows) or 1
            for w in (0, 0.25, 0.5, 0.75, 1.0):
                p = w * win["model_yes"] / model_total + (1 - w) * max(EPS, win["market_yes"]) / total
                ll[w].append(-math.log(max(EPS, p)))
        if not ll[0]:
            continue
        means = {w: sum(v) / len(v) for w, v in ll.items()}
        best = min(means, key=means.get)
        print(f"D-{lead:<4}{len(ll[0]):>7}{means[1.0]:>9.3f}{means[0]:>9.3f}   w={best} (log loss {means[best]:.3f})")

    print(f"\n--- B2. STRATEGY SIM: buy NO at (1 - price) + {spread:.3f} spread + fee, $1 payout, hold ---")

    def sim(label, lead, w, min_edge, max_fav=0.4, max_sigma=1.5, per_city=None):
        n = wins = 0
        pnl = cost_sum = 0.0
        for (city, day, l), rows in snaps.items():
            if l != lead:
                continue
            best = None
            for r in rows:
                if r["best_sigma_c"] > max_sigma or r["market_yes"] >= max_fav:
                    continue
                ask = min(0.999, 1 - r["market_yes"] + spread / 2)
                cost = ask + FEE_RATE * ask * (1 - ask)
                p_no = w * (1 - r["model_yes"]) + (1 - w) * (1 - r["market_yes"])
                if 1 - r["model_yes"] < 0.7 or p_no - cost < min_edge:
                    continue
                if best is None or p_no > best[0]:
                    best = (p_no, cost, r)
            if best:
                _, cost, r = best
                won = r["yes_result"] == 0
                n += 1
                wins += won
                pnl += (1.0 if won else 0.0) - cost
                cost_sum += cost
                if per_city is not None:
                    per_city[city][0] += 1
                    per_city[city][1] += (1.0 if won else 0.0) - cost
        if label:
            if n:
                print(f"{label:<44} trades={n:>4} win={wins / n:6.1%} pnl=${pnl:+7.2f} roi={pnl / cost_sum:+6.1%}")
            else:
                print(f"{label:<44} trades=   0")
        return n, pnl

    for lead in LEADS:
        print(f"-- D-{lead} (noon, {lead} day(s) before) --")
        for w in (1.0, 0.5):
            for e in (0.03, 0.06, 0.10):
                sim(f"model weight {w}, edge >= {e:.0%}", lead, w, e)
    per_city = defaultdict(lambda: [0, 0.0])
    for lead in LEADS:
        sim(None, lead, 0.5, 0.06, per_city=per_city)
    if per_city:
        print("\n--- B3. BY CITY (model weight 0.5, edge >= 6%, all leads) ---")
        for city, (n, pnl) in sorted(per_city.items(), key=lambda x: -x[1][1]):
            print(f"  {city:<16} trades={n:>3} pnl=${pnl:+6.2f}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--days", type=int, default=45, help="How many past days of markets to test")
    ap.add_argument("--spread", type=float, default=0.03, help="Assumed bid-ask spread")
    ap.add_argument("--offline-only", action="store_true")
    args = ap.parse_args()
    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_path.parent / db_path
    # Read the main DB without ever locking it (collector/live keep writing), and keep
    # the download cache in its own small file.
    main_db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60)
    main_db.execute("PRAGMA busy_timeout=60000")
    cache = sqlite3.connect(db_path.parent / "backtest_cache.sqlite3", timeout=60)
    cache.executescript(CACHE_DDL)
    data = load_residuals(main_db)
    if not data:
        print("station_residuals is empty: run station_history.py first.")
        return
    best = part_a(data)
    if not args.offline_only:
        part_b(main_db, cache, data, best, args.days, args.spread,
               config_path.parent / "data" / "backtest_early_trades.csv")


if __name__ == "__main__":
    main()
