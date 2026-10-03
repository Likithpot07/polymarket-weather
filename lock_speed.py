"""Step 2: is there money in being FAST on locked brackets?

A bracket is "locked" once two temperature readings are already above it: it
can no longer win, so NO is worth $1. This script finds every lock in your data
and checks what the NO price was at the moment it locked, and at a few moments
after. If NO was still cheap (e.g. 0.90) for minutes, speed is an edge.

    python lock_speed.py --days 14

Read-only (it only reads the event_outcomes cache made by evaluate_model.py).
Paste the printed summary back to Claude. Details go to data/lock_speed.csv.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from statistics import median
from zoneinfo import ZoneInfo

from edge import quoted_book
from probability import LOCK_MARGIN_C, bracket, parse_time

FEE_RATE = 0.05
OFFSETS = (("at reading", None), ("we saw it", 0), ("+1 min", 60), ("+5 min", 300),
           ("+15 min", 900), ("+60 min", 3600))
PROFIT_CAP = 0.99  # only count NO shares priced at/below this as "worth buying"


def readings(db: sqlite3.Connection, event_id: str) -> list[dict]:
    """Unique METAR readings for an event: value, when measured, when we first stored it."""
    rows = db.execute(
        """SELECT o.source_record_id,o.observed_at,o.temperature,o.unit,MIN(o.fetched_at)
           FROM observations o JOIN markets m ON m.id=o.market_id
           WHERE m.event_id=? GROUP BY o.source_record_id,o.observed_at,o.temperature,o.unit""",
        (event_id,),
    ).fetchall()
    out = {}
    for record, observed, temp, stored_unit, fetched in rows:
        key = record or observed
        # Store in Celsius; compare against brackets after converting to their unit.
        temp_c = float(temp) if stored_unit == "C" else (float(temp) - 32) * 5 / 9
        out[key] = {"observed": parse_time(observed), "fetched": parse_time(fetched), "temp_c": temp_c}
    return sorted(out.values(), key=lambda r: r["fetched"])


def lock_moment(values: list[dict], level: float, unit: str = "C") -> dict | None:
    """First moment (in the order WE learned them) two readings are >= level (in `unit`)."""
    above = []
    for r in values:
        temp = r["temp_c"] if unit == "C" else r["temp_c"] * 9 / 5 + 32
        if temp >= level:
            above.append(r)
            if len(above) == 2:
                return {"observed": max(x["observed"] for x in above), "fetched": r["fetched"]}
    return None


def history_window(db, token: str, start: datetime, end: datetime) -> list[dict]:
    """Book events from the last full snapshot before `start` up to `end`."""
    anchor = db.execute(
        """SELECT received_at FROM book_events
           WHERE token_id=? AND event_type='book' AND received_at<=?
           ORDER BY received_at DESC LIMIT 1""",
        (token, start.isoformat()),
    ).fetchone()
    begin = anchor[0] if anchor else (start - timedelta(hours=6)).isoformat()
    events = []
    for row_id, kind, received, raw, bid, ask in db.execute(
        """SELECT id,event_type,received_at,raw_json,best_bid,best_ask FROM book_events
           WHERE token_id=? AND received_at>=? AND received_at<=?
             AND event_type IN ('book','price_change','best_bid_ask')
           ORDER BY received_at,id""",
        (token, begin, end.isoformat()),
    ):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        events.append({"id": row_id, "token_id": token, "received_at": parse_time(received),
                       "event_type": kind, "raw": payload if isinstance(payload, dict) else {},
                       "best_bid": bid, "best_ask": ask})
    return events


def opportunity(quote: dict | None) -> dict:
    if not quote:
        return {"ask": None, "profit": 0.0, "shares": 0.0}
    profit = shares = 0.0
    for level in quote.get("ask_levels") or []:
        price, size = float(level["price"]), float(level["size"])
        if price > PROFIT_CAP:
            break
        profit += (1 - price - FEE_RATE * price * (1 - price)) * size
        shares += size
    return {"ask": float(quote["best_ask"]), "profit": profit, "shares": shares}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--out", default="data/lock_speed.csv")
    args = ap.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_path.parent / db_path
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    try:
        cached = {(str(e), str(m)): int(y) for e, m, y in
                  db.execute("SELECT event_id,market_id,yes_result FROM event_outcomes")}
    except sqlite3.OperationalError:
        cached = {}

    since = (datetime.now(timezone.utc) - timedelta(days=args.days)).date().isoformat()
    events = db.execute(
        """SELECT DISTINCT m.event_id,m.city,m.target_date,l.timezone FROM markets m
           JOIN locations l ON l.city=m.city
           WHERE m.target_date>=? ORDER BY m.target_date""", (since,)).fetchall()
    print(f"Events checked: {len(events)}")

    rows = []
    for event_id, city, target, tz_name in events:
        values = readings(db, str(event_id))
        if len(values) < 2:
            continue
        markets = db.execute(
            """SELECT m.id,m.question,r.unit,t.token_id FROM markets m
               JOIN rule_reviews r ON r.market_id=m.id
               JOIN tokens t ON t.market_id=m.id AND t.side='NO'
               WHERE m.event_id=?""", (event_id,)).fetchall()
        for market_id, question, unit, token in markets:
            parsed = bracket(question or "")
            if not parsed or parsed[1] is None:
                continue  # upper tail can never be locked by a high
            scale = parsed[2]  # the bracket's own unit is what settles
            margin = LOCK_MARGIN_C if scale == "C" else LOCK_MARGIN_C * 9 / 5
            level = parsed[1] + 0.5 + margin
            lock = lock_moment(values, level, scale)
            if not lock:
                continue
            times = {"at reading": lock["observed"]}
            for label, seconds in OFFSETS[1:]:
                times[label] = lock["fetched"] + timedelta(seconds=seconds)
            history = history_window(db, str(token), min(times.values()), max(times.values()))
            if not history:
                continue
            row = {"event_id": event_id, "city": city, "target_date": target, "market_id": market_id,
                   "bracket": question,
                   "lock_local": lock["observed"].astimezone(ZoneInfo(tz_name)).strftime("%H:%M"),
                   "our_delay_min": round((lock["fetched"] - lock["observed"]).total_seconds() / 60, 1),
                   "resolved_yes": cached.get((str(event_id), str(market_id)))}
            for label, moment in times.items():
                opp = opportunity(quoted_book(history, moment, 10 ** 9))
                row[f"ask {label}"] = opp["ask"]
                row[f"$ {label}"] = round(opp["profit"], 2)
            rows.append(row)

    if not rows:
        print("No locked brackets with order-book data found.")
        return
    out = Path(args.out)
    out = out if out.is_absolute() else config_path.parent / out
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summarize(rows)


def summarize(rows: list[dict]) -> None:
    print(f"Locked brackets found: {len(rows)}")
    known = [r for r in rows if r["resolved_yes"] is not None]
    failed = [r for r in known if r["resolved_yes"] == 1]
    print(f"Settled so far: {len(known)} | locked but resolved YES (should be 0): {len(failed)}")
    for r in failed[:5]:
        print("  FAILED LOCK:", r["city"], r["target_date"], r["bracket"])
    delays = [r["our_delay_min"] for r in rows]
    print(f"How late we learned about the reading: median {median(delays):.1f} min, worst {max(delays):.1f} min")

    print("\nNO price and money on the table (profit if you bought every NO share priced <= 0.99):")
    print(f"{'moment':<12}{'median ask':>11}{'ask<0.98':>10}{'ask<0.95':>10}{'total $':>10}")
    for label, _ in OFFSETS:
        asks = [r[f"ask {label}"] for r in rows if r[f"ask {label}"] is not None]
        money = sum(r[f"$ {label}"] for r in rows)
        if not asks:
            continue
        cheap98 = sum(a < 0.98 for a in asks) / len(asks)
        cheap95 = sum(a < 0.95 for a in asks) / len(asks)
        print(f"{label:<12}{median(asks):>11.3f}{cheap98:>10.0%}{cheap95:>10.0%}{money:>10.2f}")

    best = sorted(rows, key=lambda r: r["$ we saw it"], reverse=True)[:8]
    print("\nBiggest chances when we saw it:")
    for r in best:
        print(f"  {r['city']:<13} {r['target_date']} lock {r['lock_local']} | ask {r['ask we saw it']} "
              f"| ${r['$ we saw it']} | +5min ask {r['ask +5 min']} | {str(r['bracket'])[:60]}")


if __name__ == "__main__":
    main()
