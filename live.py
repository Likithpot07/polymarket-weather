"""Continuous paper/live signal runner. Reads collector data; never places orders."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

from decide import event_decisions
from filters import Policy

LIVE_DDL = """
CREATE TABLE IF NOT EXISTS live_decision_runs (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    as_of TEXT NOT NULL,
    city TEXT NOT NULL,
    target_date TEXT NOT NULL,
    counts_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS live_run_event_time
ON live_decision_runs(event_id,as_of);

CREATE TABLE IF NOT EXISTS live_decisions (
    run_id INTEGER NOT NULL REFERENCES live_decision_runs(id),
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    model_probability REAL,
    best_bid REAL,
    best_ask REAL,
    ask_size REAL,
    book_age_seconds REAL,
    quote_source TEXT,
    depth_verified INTEGER NOT NULL,
    entry_cost REAL,
    net_edge REAL,
    reasons_json TEXT NOT NULL,
    bracket TEXT,
    token_id TEXT,
    PRIMARY KEY(run_id,market_id,side)
);

CREATE TABLE IF NOT EXISTS live_signal_state (
    event_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(event_id,market_id,side)
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    status TEXT NOT NULL,
    fill_assumed INTEGER NOT NULL DEFAULT 0,
    model_probability REAL NOT NULL,
    best_ask REAL NOT NULL,
    ask_size REAL,
    entry_cost REAL NOT NULL,
    net_edge REAL NOT NULL,
    quote_source TEXT,
    depth_verified INTEGER NOT NULL,
    bracket TEXT,
    token_id TEXT,
    close_reason TEXT
);
CREATE INDEX IF NOT EXISTS paper_trade_event_time
ON paper_trades(event_id,opened_at);
"""


def active_event_ids(db: sqlite3.Connection, as_of: datetime) -> list[str]:
    """Return only events whose target day has not passed in that city's timezone."""
    rows = db.execute(
        """SELECT DISTINCT m.event_id,m.target_date,l.timezone
           FROM markets m
           LEFT JOIN locations l ON l.city=m.city
           WHERE m.active=1
             AND m.city IS NOT NULL
             AND m.target_date IS NOT NULL
           ORDER BY m.target_date,m.event_id"""
    ).fetchall()
    result = []
    for event_id, target_date, timezone_name in rows:
        if not timezone_name:
            continue
        try:
            from datetime import date
            from zoneinfo import ZoneInfo
            local_day = as_of.astimezone(ZoneInfo(str(timezone_name))).date()
            if local_day <= date.fromisoformat(str(target_date)):
                result.append(str(event_id))
        except (ValueError, TypeError):
            continue
    return result


def persist_snapshot(
    db: sqlite3.Connection,
    state: dict,
    decisions: list[dict],
    as_of: datetime,
) -> list[dict]:
    """Persist one evaluation and return only newly-entered TRADE signals."""
    counts = dict(Counter(d["action"] for d in decisions))
    cursor = db.execute(
        """INSERT INTO live_decision_runs(
               event_id,as_of,city,target_date,counts_json,created_at
           ) VALUES (?,?,?,?,?,?)""",
        (
            state["event_id"],
            as_of.isoformat(),
            state["city"],
            state["target_date"],
            json.dumps(counts, separators=(",", ":")),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    run_id = cursor.lastrowid
    new_trades: list[dict] = []

    for decision in decisions:
        db.execute(
            """INSERT INTO live_decisions(
                   run_id,market_id,side,action,model_probability,best_bid,best_ask,
                   ask_size,book_age_seconds,quote_source,depth_verified,entry_cost,
                   net_edge,reasons_json,bracket,token_id
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                decision["market_id"],
                decision["side"],
                decision["action"],
                decision.get("model_probability"),
                decision.get("best_bid"),
                decision.get("best_ask"),
                decision.get("ask_size"),
                decision.get("book_age_seconds"),
                decision.get("quote_source"),
                int(bool(decision.get("depth_verified"))),
                decision.get("entry_cost"),
                decision.get("net_edge"),
                json.dumps(decision.get("reasons", []), separators=(",", ":")),
                decision.get("bracket"),
                decision.get("token_id"),
            ),
        )

        key = (
            state["event_id"],
            decision["market_id"],
            decision["side"],
        )
        previous = db.execute(
            """SELECT action FROM live_signal_state
               WHERE event_id=? AND market_id=? AND side=?""",
            key,
        ).fetchone()
        previous_action = previous[0] if previous else None

        if decision["action"] == "TRADE" and previous_action != "TRADE":
            if (
                decision.get("best_ask") is not None
                and decision.get("entry_cost") is not None
                and decision.get("net_edge") is not None
            ):
                db.execute(
                    """INSERT INTO paper_trades(
                           event_id,market_id,side,opened_at,status,fill_assumed,
                           model_probability,best_ask,ask_size,entry_cost,net_edge,
                           quote_source,depth_verified,bracket,token_id
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        state["event_id"],
                        decision["market_id"],
                        decision["side"],
                        as_of.isoformat(),
                        "OPEN_SIGNAL",
                        0,
                        decision["model_probability"],
                        decision["best_ask"],
                        decision.get("ask_size"),
                        decision["entry_cost"],
                        decision["net_edge"],
                        decision.get("quote_source"),
                        int(bool(decision.get("depth_verified"))),
                        decision.get("bracket"),
                        decision.get("token_id"),
                    ),
                )
                new_trades.append(decision)

        if previous_action == "TRADE" and decision["action"] != "TRADE":
            open_row = db.execute(
                """SELECT id FROM paper_trades
                   WHERE event_id=? AND market_id=? AND side=?
                     AND status='OPEN_SIGNAL'
                   ORDER BY id DESC LIMIT 1""",
                key,
            ).fetchone()
            if open_row:
                db.execute(
                    """UPDATE paper_trades
                       SET closed_at=?,status='CLOSED_SIGNAL',close_reason=?
                       WHERE id=?""",
                    (
                        as_of.isoformat(),
                        decision["action"] + ":" + ",".join(decision.get("reasons", [])),
                        open_row[0],
                    ),
                )

        db.execute(
            """INSERT INTO live_signal_state(event_id,market_id,side,action,updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(event_id,market_id,side) DO UPDATE SET
                   action=excluded.action,
                   updated_at=excluded.updated_at""",
            (*key, decision["action"], as_of.isoformat()),
        )

    return new_trades


def format_trade(decision: dict) -> str:
    ask = decision.get("best_ask")
    depth = decision.get("ask_size")
    edge = decision.get("net_edge")
    probability = decision.get("model_probability")
    ask_text = "NA" if ask is None else f"{ask:.3f}"
    depth_text = "NA" if depth is None else f"{depth:.2f}"
    edge_text = "NA" if edge is None else f"{edge * 100:.2f}%"
    probability_text = "NA" if probability is None else f"{probability * 100:.2f}%"
    return (
        f"{decision['side']} | p={probability_text} | ask={ask_text} | "
        f"depth={depth_text} | net_edge={edge_text} | {decision.get('bracket')}"
    )


def run_cycle(
    db: sqlite3.Connection,
    policy: Policy,
    event_ids: list[str] | None,
    as_of: datetime,
) -> dict:
    ids = event_ids or active_event_ids(db, as_of)
    summary = {"events": 0, "trade": 0, "watch": 0, "skip": 0, "errors": 0, "new_trades": []}

    for event_id in ids:
        try:
            state, decisions = event_decisions(db, event_id, as_of, policy)
            counts = Counter(d["action"] for d in decisions)
            with db:
                new_trades = persist_snapshot(db, state, decisions, as_of)
            summary["events"] += 1
            summary["trade"] += counts.get("TRADE", 0)
            summary["watch"] += counts.get("WATCH", 0)
            summary["skip"] += counts.get("SKIP", 0)
            summary["new_trades"].extend(
                [{"event_id": event_id, **decision} for decision in new_trades]
            )
            print(
                f"{as_of.isoformat()} | {event_id} | {state['city']} | "
                f"{state['target_date']} | TRADE={counts.get('TRADE', 0)} "
                f"WATCH={counts.get('WATCH', 0)} SKIP={counts.get('SKIP', 0)}",
                flush=True,
            )
            current_trades = [d for d in decisions if d["action"] == "TRADE"]
            for decision in current_trades:
                prefix = "NEW TRADE SIGNAL" if decision in new_trades else "TRADE NOW"
                print(f"{prefix} | {format_trade(decision)}", flush=True)
        except (ValueError, sqlite3.Error) as exc:
            summary["errors"] += 1
            print(f"{as_of.isoformat()} | {event_id} | ERROR | {exc}", flush=True)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--event-id", action="append", help="Limit to event ID; repeatable")
    parser.add_argument("--interval", type=float, help="Seconds between decision cycles")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
    if not db_path.is_file():
        parser.error(f"Collector database does not exist: {db_path}")

    interval = args.interval if args.interval is not None else float(cfg.get("live_interval_seconds", 30))
    if interval <= 0:
        parser.error("--interval must be positive")

    policy = Policy.from_config(cfg)
    requested_events = list(dict.fromkeys(args.event_id or [])) or None

    db = sqlite3.connect(db_path, timeout=30)
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript(LIVE_DDL)

    print(
        "LIVE PAPER SIGNALS | no orders are placed | "
        f"interval={interval:g}s | events={'all active' if requested_events is None else ','.join(requested_events)}",
        flush=True,
    )

    try:
        while True:
            cycle_started = time.monotonic()
            now = datetime.now(timezone.utc)
            summary = run_cycle(db, policy, requested_events, now)
            print(
                f"CYCLE | events={summary['events']} TRADE={summary['trade']} "
                f"WATCH={summary['watch']} SKIP={summary['skip']} "
                f"errors={summary['errors']} new={len(summary['new_trades'])}",
                flush=True,
            )
            if args.once:
                break
            elapsed = time.monotonic() - cycle_started
            time.sleep(max(0.0, interval - elapsed))
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
