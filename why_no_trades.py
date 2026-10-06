"""Quick health check: is the collector feeding fresh data, and why isn't the bot trading?

    python why_no_trades.py
    python why_no_trades.py --size     # what is taking space in the database (fast estimate)

Read-only. Paste the output back to Claude.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import sys
from zoneinfo import ZoneInfo


def age(db, sql: str) -> str:
    value = db.execute(sql).fetchone()[0]
    if not value:
        return "never"
    when = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    minutes = (datetime.now(timezone.utc) - when).total_seconds() / 60
    flag = "  <-- STALE" if minutes > 60 else ""
    return f"{minutes:.0f} min ago{flag}"


SIZE_TABLES = {  # table -> timestamp column
    "book_events": "received_at", "live_decisions": None, "live_decision_runs": "as_of",
    "live_model_diagnostics": "as_of", "paper_candidate_audit": "as_of", "live_signal_state": None,
    "paper_trades": "opened_at", "forecast_batches": "fetched_at", "forecast_values": None,
    "observations": "fetched_at", "calibration_observations": None, "markets": None,
    "events": None, "tokens": None, "paper_orders": "signal_at",
}


def size_report(db, db_path: Path) -> None:
    """Fast estimate: rows = highest rowid, bytes/row from the newest 2000 rows."""
    print("=== FILES ===")
    for f in sorted(db_path.parent.glob("*.sqlite3*")):
        print(f"  {f.name:<32} {f.stat().st_size / 1e9:8.2f} GB")
    print("\n=== TABLES (estimated, biggest first) ===")
    rows_out = []
    for table, ts in SIZE_TABLES.items():
        try:
            hi = db.execute(f"SELECT MAX(rowid) FROM {table}").fetchone()[0]  # separate queries:
            lo = db.execute(f"SELECT MIN(rowid) FROM {table}").fetchone()[0]  # each is instant
        except sqlite3.Error:
            continue
        if hi is None:
            continue
        sample = db.execute(f"SELECT * FROM {table} WHERE rowid>?", (hi - 2000,)).fetchall()
        per_row = sum(sum(len(str(v)) for v in r if v is not None) for r in sample) / max(1, len(sample))
        span = ""
        if ts:
            try:
                first = db.execute(f"SELECT {ts} FROM {table} WHERE rowid=?", (lo,)).fetchone()
                last = db.execute(f"SELECT {ts} FROM {table} WHERE rowid=?", (hi,)).fetchone()
                if first and last:
                    span = f"{str(first[0])[:10]} -> {str(last[0])[:10]}"
            except sqlite3.Error:
                pass
        rows_out.append((hi * per_row, table, hi, per_row, span))
    for est, table, n, per_row, span in sorted(rows_out, reverse=True):
        print(f"  {table:<24} ~{n / 1e6:9.2f}M rows x {per_row:6.0f} B = ~{est / 1e9:7.2f} GB   {span}")
    print("\nEstimates only (deleted rows and indexes not counted). Read-only.")


def main() -> None:
    cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
    db = sqlite3.connect(f"file:{cfg['db_path']}?mode=ro", uri=True, timeout=60)
    if "--size" in sys.argv:
        size_report(db, Path(cfg["db_path"]).resolve())
        return
    print("=== 1. IS THE COLLECTOR ALIVE? (each should be recent) ===")
    print("last order-book update :", age(db, "SELECT received_at FROM book_events WHERE id=(SELECT MAX(id) FROM book_events)"))
    print("last forecast download :", age(db, "SELECT fetched_at FROM forecast_batches WHERE id=(SELECT MAX(id) FROM forecast_batches)"))
    print("last temperature read  :", age(db, "SELECT fetched_at FROM observations WHERE id=(SELECT MAX(id) FROM observations)"))
    print("last live.py decision  :", age(db, "SELECT as_of FROM live_decision_runs WHERE id=(SELECT MAX(id) FROM live_decision_runs)"))

    print("\n=== 2. MARKETS THE BOT KNOWS ABOUT, BY DAYS UNTIL TARGET DATE ===")
    now = datetime.now(timezone.utc)
    events = db.execute(
        """SELECT DISTINCT m.event_id,m.city,m.target_date,l.timezone FROM markets m
           JOIN locations l ON l.city=m.city WHERE m.active=1 AND m.target_date IS NOT NULL""").fetchall()
    lead_of = {}
    for event_id, city, target, tz in events:
        lead = (date.fromisoformat(target) - now.astimezone(ZoneInfo(tz)).date()).days
        if lead >= 0:
            lead_of[str(event_id)] = lead
    counts = Counter(lead_of.values())
    for lead in sorted(counts):
        print(f"  {lead} day(s) ahead: {counts[lead]} cities")

    print("\n=== 3. WHY NO TRADES? latest decision per event, NO side, by days ahead ===")
    print("(each count = number of brackets blocked by that reason; one bracket can have several)")
    reasons_by_lead: dict[int, Counter] = {}
    actions_by_lead: dict[int, Counter] = {}
    for event_id, lead in lead_of.items():
        run = db.execute("SELECT MAX(id) FROM live_decision_runs WHERE event_id=?", (event_id,)).fetchone()[0]
        if not run:
            reasons_by_lead.setdefault(lead, Counter())["NOT EVALUATED BY live.py YET"] += 1
            continue
        for action, reasons in db.execute(
                "SELECT action,reasons_json FROM live_decisions WHERE run_id=? AND side='NO'", (run,)):
            actions_by_lead.setdefault(lead, Counter())[action] += 1
            for reason in json.loads(reasons or "[]"):
                reasons_by_lead.setdefault(lead, Counter())[reason] += 1
    for lead in sorted(set(reasons_by_lead) | set(actions_by_lead)):
        print(f"\n  -- {lead} day(s) ahead | actions: {dict(actions_by_lead.get(lead, {}))}")
        for reason, n in reasons_by_lead.get(lead, Counter()).most_common(12):
            print(f"     {reason:<34}{n}")

    print("\n=== 3b. FORECAST UNCERTAINTY FOR MARKETS 2 DAYS AHEAD (trade only if <= 1.5C) ===")
    for event_id, lead in sorted(lead_of.items()):
        if lead != 2:
            continue
        row = db.execute(
            """SELECT r.city,d.diagnostics_json FROM live_model_diagnostics d
               JOIN live_decision_runs r ON r.id=d.run_id
               WHERE d.run_id=(SELECT MAX(id) FROM live_decision_runs WHERE event_id=?)""",
            (event_id,)).fetchone()
        if not row:
            continue
        diag = json.loads(row[1] or "{}")
        combined = diag.get("combined")
        unit = "F" if any(c.get("mean", 0) > 60 for c in diag.get("components", [])) else "C"
        if combined:
            sigma_c = combined["sigma"] * (5 / 9 if unit == "F" else 1)
            print(f"  {row[0]:<16} combined forecast, typical miss {sigma_c:.2f}C")
        else:
            print(f"  {row[0]:<16} NO station history fit (old fallback used)")

    print("\n=== 4. PAPER ORDERS IN THE LAST 24 HOURS ===")
    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    for status, n in db.execute(
            "SELECT status,COUNT(*) FROM paper_orders WHERE signal_at >= ? GROUP BY status", (since,)):
        print(f"  {status}: {n}")
    print("\nSettings in use:", json.dumps(cfg.get("decision", {})))


if __name__ == "__main__":
    main()
