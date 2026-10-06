"""List every open paper bet: when it should settle (India time) and how it's doing right now.

    python open_bets.py            # all open bets, soonest first
    python open_bets.py --today    # only bets expected to settle today (India time)

Read-only. "Now worth" uses the current best bid for the NO share (what you could
sell it for this minute); a settled winner pays $1 per share.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

from edge import quoted_book, recent_book_history

IST = ZoneInfo("Asia/Kolkata")
SETTLE_DELAY_HOURS = 3  # Polymarket usually confirms a few hours after the day ends


def short_bracket(question: str) -> str:
    q = question or ""
    m = re.search(r"\bbe (.+?)(?: on |\?|$)", q)
    return (m.group(1) if m else q)[:22]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--today", action="store_true", help="Only bets expected to settle today (India time)")
    args = ap.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    db = sqlite3.connect(f"file:{cfg['db_path']}?mode=ro", uri=True, timeout=60)

    rows = db.execute(
        """SELECT p.token_id,p.filled_shares,p.total_cost,p.vwap,p.model_probability,
                  m.city,m.target_date,m.question,l.timezone
           FROM paper_orders p JOIN markets m ON m.id=p.market_id
           LEFT JOIN locations l ON l.city=m.city
           WHERE p.status IN ('FILLED','PARTIAL') AND p.settled_at IS NULL AND p.legacy=0""").fetchall()
    now = datetime.now(timezone.utc)
    today_ist = now.astimezone(IST).date()
    bets = []
    for token, shares, cost, vwap, p_model, city, target, question, tz_name in rows:
        tz = ZoneInfo(tz_name or "UTC")
        day_end = datetime.combine(date.fromisoformat(target) + timedelta(days=1), time(0), tz)
        settle = (day_end + timedelta(hours=SETTLE_DELAY_HOURS)).astimezone(IST)
        if args.today and settle.date() != today_ist:
            continue
        quote = quoted_book(recent_book_history(db, {str(token)}, now)[str(token)], now, 10 ** 9)
        bid = quote["best_bid"] if quote else None
        worth = None if bid is None else float(shares) * float(bid)
        bets.append((settle, city, target, short_bracket(question), float(cost), float(vwap or 0),
                     p_model, bid, worth))
    bets.sort()
    if not bets:
        print("No open bets" + (" settling today." if args.today else "."))
        return

    print(f"{'settles ~ (IST)':<17}{'city':<15}{'day':<12}{'bet NO on':<24}{'paid':>7}{'price':>7}"
          f"{'model':>7}{'now':>6}{'now worth':>11}")
    total_cost = total_worth = 0.0
    for settle, city, target, label, cost, vwap, p_model, bid, worth in bets:
        total_cost += cost
        total_worth += worth or 0.0
        print(f"{settle:%d %b %H:%M}     {city[:14]:<15}{target:<12}{label:<24}${cost:>6.2f}"
              f"{vwap:>7.3f}{(p_model or 0):>7.0%}{('-' if bid is None else f'{bid:.2f}'):>6}"
              f"{('-' if worth is None else f'${worth:.2f}'):>11}")
    print(f"\n{len(bets)} open bets | paid ${total_cost:.2f} | worth now about ${total_worth:.2f} "
          f"({total_worth - total_cost:+.2f}) if sold at today's bids")
    print("'price' = what we paid per NO share; 'model' = our chance NO wins; 'now' = current bid for NO.")


if __name__ == "__main__":
    main()
