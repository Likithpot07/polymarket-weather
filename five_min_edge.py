"""Test: would 5-minute airport readings let us lock brackets before the market moves?

US airports also publish readings every 5 minutes ("high-frequency METAR"), not just
the hourly report your bot uses. If the 5-minute data shows the temperature already
past a bracket, that bracket will very likely be ruled out once the next hourly
report comes in. This script checks, on the last N days of US markets:

  1. How often a 5-minute "signal" (two readings in a row past a bracket) was RIGHT
     (the bracket really lost).
  2. How many minutes earlier the signal came than the hourly report.
  3. What the NO price was at the signal (plus a delay you choose for getting the
     data), and how much profit was available, counting wrong signals as losses.

    python five_min_edge.py --days 10 --delay-min 5

Reads your DB (only adds settled results to event_outcomes); 5-minute data is cached
in data/fivemin_cache.sqlite3. Paste the summary back to Claude.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
import sqlite3
from statistics import median
from zoneinfo import ZoneInfo

from collector import station_coordinates  # noqa: F401  (installs DNS/SSL setup used by other scripts)
from edge import quoted_book, recent_book_history
from evaluate_model import OUTCOME_DDL, outcomes_for
from lock_speed import lock_moment, readings
from probability import LOCK_MARGIN_C, bracket
from station_history import http_get, IEM_ASOS

FEE_RATE = 0.05
MAX_PRICE = 0.97          # only buy NO shares priced at or below this
MAX_SHARES = 100          # per signal
CACHE_DDL = "CREATE TABLE IF NOT EXISTS fivemin (station TEXT, day TEXT, payload TEXT, PRIMARY KEY(station, day));"


IEM_ONE_MIN = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"
DIAG: dict[str, int] = {}
SHOWN_SAMPLE = False


def _parse(text: str, day: date, tz: ZoneInfo) -> list[tuple[datetime, float]]:
    out = []
    for rec in csv.DictReader(line for line in io.StringIO(text) if not line.startswith("#")):
        valid = next((v for k, v in rec.items() if k and k.lower().startswith("valid")), None)
        temp = next((v for k, v in rec.items() if k and k.lower().startswith("tmpf")), None)
        try:
            when = datetime.strptime(str(valid)[:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            temp_c = (float(temp) - 32) * 5 / 9
        except (TypeError, ValueError):
            continue
        if when.astimezone(tz).date() == day:
            out.append((when, temp_c))
    out.sort()
    return out


def five_minute_readings(cache, station: str, day: date, tz: ZoneInfo) -> list[tuple[datetime, float]]:
    """(time, temp_C) for the local day. Tries IEM high-frequency METAR, then IEM 1-minute ASOS."""
    global SHOWN_SAMPLE
    row = cache.execute("SELECT payload FROM fivemin WHERE station=? AND day=?", (station, day.isoformat())).fetchone()
    if row:
        cached = [(datetime.fromisoformat(t), v) for t, v in json.loads(row[0])]
        if cached:
            DIAG["from cache"] = DIAG.get("from cache", 0) + 1
            return cached
    code = station[1:] if len(station) == 4 and station.startswith("K") else station
    start, end = day - timedelta(days=1), day + timedelta(days=1)
    dates = (f"&year1={start.year}&month1={start.month}&day1={start.day}"
             f"&year2={end.year}&month2={end.month}&day2={end.day}")
    sources = [
        ("high-frequency METAR", f"{IEM_ASOS}?station={code}&data=tmpf&tz=Etc/UTC&format=onlycomma"
                                 f"&latlon=no&missing=M&report_type=1{dates}"),
        ("1-minute ASOS", f"{IEM_ONE_MIN}?station={code}&vars=tmpf&sample=5min&what=download"
                          f"&delim=comma&gis=no&tz=UTC&sts={start}T00:00Z&ets={end}T00:00Z"),
    ]
    out = []
    for name, url in sources:
        try:
            text = http_get(url, attempts=2).decode("utf-8", "replace")
        except Exception as exc:
            DIAG[f"{name}: download error"] = DIAG.get(f"{name}: download error", 0) + 1
            if not SHOWN_SAMPLE:
                print(f"  sample error from {name}: {exc}")
            continue
        out = _parse(text, day, tz)
        if out:
            DIAG[f"{name}: ok"] = DIAG.get(f"{name}: ok", 0) + 1
            break
        DIAG[f"{name}: empty"] = DIAG.get(f"{name}: empty", 0) + 1
        if not SHOWN_SAMPLE:
            print(f"  sample empty response from {name} ({station} {day}): {text[:200]!r}")
    SHOWN_SAMPLE = True
    if out:  # never cache an empty result
        with cache:
            cache.execute("INSERT OR REPLACE INTO fivemin VALUES (?,?,?)",
                          (station, day.isoformat(), json.dumps([(t.isoformat(), v) for t, v in out])))
    return out


def signal_time(values, level: float, unit: str):
    """First time two consecutive 5-minute readings are at/above level."""
    previous = False
    for when, temp_c in values:
        temp = temp_c if unit == "C" else temp_c * 9 / 5 + 32
        above = temp >= level
        if above and previous:
            return when
        previous = above
    return None


def buy_no(quote) -> tuple[float, float]:
    """(shares, cost) buying NO up to MAX_PRICE / MAX_SHARES."""
    shares = cost = 0.0
    for level in (quote or {}).get("ask_levels") or []:
        price, size = float(level["price"]), float(level["size"])
        if price > MAX_PRICE or shares >= MAX_SHARES:
            break
        take = min(size, MAX_SHARES - shares)
        shares += take
        cost += take * (price + FEE_RATE * price * (1 - price))
    return shares, cost


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--delay-min", type=float, default=5, help="Minutes to get and act on a 5-min reading")
    args = ap.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    db.executescript(OUTCOME_DDL)
    cache = sqlite3.connect(db_path.parent / "fivemin_cache.sqlite3")
    cache.executescript(CACHE_DDL)

    today = datetime.now(timezone.utc).date()
    events = db.execute(
        """SELECT DISTINCT m.event_id,m.city,m.target_date,l.timezone,r.station FROM markets m
           JOIN rule_reviews r ON r.market_id=m.id JOIN locations l ON l.city=m.city
           WHERE r.station LIKE 'K%' AND m.target_date>=? AND m.target_date<?
           ORDER BY m.target_date""",
        ((today - timedelta(days=args.days)).isoformat(), (today - timedelta(days=1)).isoformat())).fetchall()
    print(f"US events to check: {len(events)}", flush=True)

    rows = []
    for index, (event_id, city, target, tz_name, station) in enumerate(events, 1):
        tz = ZoneInfo(tz_name)
        try:
            five = five_minute_readings(cache, station, date.fromisoformat(target), tz)
        except Exception as exc:
            print(f"  {city} {target}: 5-minute data failed ({exc})")
            continue
        if not five:
            continue
        outcome = outcomes_for(db, str(event_id), offline=False)
        hourly = readings(db, str(event_id))
        markets = db.execute(
            """SELECT m.id,m.question,t.token_id FROM markets m
               JOIN tokens t ON t.market_id=m.id AND t.side='NO' WHERE m.event_id=?""", (event_id,)).fetchall()
        for market_id, question, token in markets:
            parsed = bracket(question or "")
            if not parsed or parsed[1] is None:
                continue
            unit = parsed[2]
            level = parsed[1] + 0.5 + (LOCK_MARGIN_C if unit == "C" else LOCK_MARGIN_C * 9 / 5)
            t5 = signal_time(five, level, unit)
            if not t5:
                continue
            act = t5 + timedelta(minutes=args.delay_min)
            lock = lock_moment(hourly, level, unit) if hourly else None
            result = outcome.get(str(market_id))
            quote = quoted_book(recent_book_history(db, {str(token)}, act)[str(token)], act, 10 ** 9)
            shares, cost = buy_no(quote)
            if result is None:
                correct = None
                pnl = None
            else:
                correct = result == 0
                pnl = (shares if correct else 0.0) - cost
            rows.append({
                "city": city, "day": target, "bracket": question,
                "signal_local": t5.astimezone(tz).strftime("%H:%M"),
                "minutes_before_hourly_lock": round((lock["observed"] - t5).total_seconds() / 60, 1) if lock else None,
                "correct": correct, "no_ask_at_action": quote["best_ask"] if quote else None,
                "shares": shares, "cost": round(cost, 2), "pnl": None if pnl is None else round(pnl, 2),
            })
        if index % 20 == 0:
            print(f"  {index}/{len(events)} events checked, {len(rows)} signals so far", flush=True)

    print("5-minute data sources:", DIAG)
    if not rows:
        print("No 5-minute signals found. If the sources above say 'empty' or 'error', the data didn't download;"
              " paste this output to Claude.")
        return
    out = db_path.parent / "five_min_edge.csv"
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    judged = [r for r in rows if r["correct"] is not None]
    right = [r for r in judged if r["correct"]]
    leads = [r["minutes_before_hourly_lock"] for r in rows if r["minutes_before_hourly_lock"] is not None]
    asks = [r["no_ask_at_action"] for r in rows if r["no_ask_at_action"] is not None]
    traded = [r for r in judged if r["shares"] > 0]
    print(f"\n=== 5-MINUTE SIGNAL TEST (last {args.days} days, US airports, acting {args.delay_min:g} min after the reading) ===")
    print(f"Signals found: {len(rows)}  (settled so far: {len(judged)})")
    if judged:
        print(f"Signals that were RIGHT (bracket really lost): {len(right)}/{len(judged)} = {len(right) / len(judged):.1%}")
    if leads:
        print(f"How much earlier than the hourly report: median {median(leads):.0f} min "
              f"(range {min(leads):.0f} to {max(leads):.0f})")
    if asks:
        print(f"NO price when we would act: median {median(asks):.3f}; "
              f"below 0.97 in {sum(a <= MAX_PRICE for a in asks) / len(asks):.0%} of signals")
    if traded:
        pnl = sum(r["pnl"] for r in traded)
        cost = sum(r["cost"] for r in traded)
        wins = sum(r["correct"] for r in traded)
        print(f"Simulated trades (NO <= {MAX_PRICE}, up to {MAX_SHARES} shares): {len(traded)} trades, "
              f"{wins} right, {len(traded) - wins} wrong | profit ${pnl:+.2f} on ${cost:.2f} ({100 * pnl / cost:+.1f}%)")
    else:
        print("Simulated trades: none (NO was already above 0.97 every time)")
    print(f"\nDetails: {out}")


if __name__ == "__main__":
    main()
