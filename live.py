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
from paper import PAPER_DDL, best_no_trade, city_day_gate, process_pending_orders, schedule_order, settle_positions, stats as paper_stats

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

CREATE TABLE IF NOT EXISTS paper_candidate_audit (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES live_decision_runs(id),
    event_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    as_of TEXT NOT NULL,
    selected INTEGER NOT NULL,
    selection_reason TEXT NOT NULL,
    model_probability REAL,
    best_ask REAL,
    net_edge REAL,
    bracket TEXT,
    UNIQUE(run_id,market_id,side)
);
CREATE INDEX IF NOT EXISTS paper_candidate_event_time
ON paper_candidate_audit(event_id,as_of);
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


def persist_candidate_audit(
    db: sqlite3.Connection,
    state: dict,
    decisions: list[dict],
    as_of: datetime,
    scheduled_trade: dict | None,
    allowed: bool,
    gate_reason: str | None,
) -> None:
    """Persist why each eligible NO trade candidate was or was not selected."""
    row = db.execute(
        """SELECT id FROM live_decision_runs
           WHERE event_id=? AND as_of=?
           ORDER BY id DESC LIMIT 1""",
        (state["event_id"], as_of.isoformat()),
    ).fetchone()
    if not row:
        raise sqlite3.Error("live decision run missing for candidate audit")
    run_id = int(row[0])
    current_trades = [
        d for d in decisions
        if d.get("action") == "TRADE" and str(d.get("side")).upper() == "NO"
    ]
    best = best_no_trade(current_trades)
    for decision in current_trades:
        selected = scheduled_trade is decision
        if selected:
            reason = "SELECTED_AND_SCHEDULED"
        elif best is not None and decision is not best:
            reason = "LOWER_EDGE_THAN_SELECTED_NO"
        elif not allowed:
            reason = gate_reason or "CITY_DAY_GATE_BLOCKED"
        else:
            reason = "PAPER_ORDER_NOT_SCHEDULED"
        db.execute(
            """INSERT OR REPLACE INTO paper_candidate_audit(
                   run_id,event_id,market_id,side,as_of,selected,selection_reason,
                   model_probability,best_ask,net_edge,bracket
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                state["event_id"],
                decision["market_id"],
                decision["side"],
                as_of.isoformat(),
                int(selected),
                reason,
                decision.get("model_probability"),
                decision.get("best_ask"),
                decision.get("net_edge"),
                decision.get("bracket"),
            ),
        )


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
    as_of: datetime | None,
    cfg: dict,
) -> dict:
    discovery_time = as_of or datetime.now(timezone.utc)
    ids = event_ids or active_event_ids(db, discovery_time)
    summary = {"events": 0, "trade": 0, "watch": 0, "skip": 0, "errors": 0, "new_trades": []}

    for event_id in ids:
        evaluation_time = as_of or datetime.now(timezone.utc)
        try:
            state, decisions = event_decisions(db, event_id, evaluation_time, policy)
            counts = Counter(d["action"] for d in decisions)
            scheduled_trade = None
            gate_reason = None
            with db:
                new_trades = persist_snapshot(db, state, decisions, evaluation_time)
                best = best_no_trade(decisions)
                allowed, gate_reason = city_day_gate(
                    db,
                    state["city"],
                    state["target_date"],
                    decisions,
                    cfg,
                )
                if allowed and best:
                    order_id = schedule_order(
                        db,
                        state,
                        best,
                        datetime.now(timezone.utc),
                        cfg,
                    )
                    if order_id:
                        scheduled_trade = best
                persist_candidate_audit(
                    db,
                    state,
                    decisions,
                    evaluation_time,
                    scheduled_trade,
                    allowed,
                    gate_reason,
                )
            summary["events"] += 1
            summary["trade"] += counts.get("TRADE", 0)
            summary["watch"] += counts.get("WATCH", 0)
            summary["skip"] += counts.get("SKIP", 0)
            summary["new_trades"].extend(
                [{"event_id": event_id, **decision} for decision in new_trades]
            )
            print(
                f"{evaluation_time.isoformat()} | {event_id} | {state['city']} | "
                f"{state['target_date']} | TRADE={counts.get('TRADE', 0)} "
                f"WATCH={counts.get('WATCH', 0)} SKIP={counts.get('SKIP', 0)}",
                flush=True,
            )
            current_trades = [
                d for d in decisions
                if d["action"] == "TRADE" and str(d["side"]).upper() == "NO"
            ]
            for decision in current_trades:
                prefix = "NEW TRADE SIGNAL" if decision in new_trades else "TRADE NOW"
                print(f"{prefix} | {format_trade(decision)}", flush=True)
            if scheduled_trade:
                print(
                    f"PAPER ORDER SCHEDULED | {format_trade(scheduled_trade)} | "
                    f"gate={gate_reason}",
                    flush=True,
                )
            if current_trades:
                best = best_no_trade(current_trades)
                for decision in current_trades:
                    if scheduled_trade is decision:
                        continue
                    if best is not None and decision is not best:
                        reason = "LOWER_EDGE_THAN_SELECTED_NO"
                    elif not allowed:
                        reason = gate_reason or "CITY_DAY_GATE_BLOCKED"
                    else:
                        reason = "PAPER_ORDER_NOT_SCHEDULED"
                    print(
                        f"PAPER TRADE SKIPPED | {format_trade(decision)} | reason={reason}",
                        flush=True,
                    )
        except (ValueError, sqlite3.Error) as exc:
            summary["errors"] += 1
            print(f"{evaluation_time.isoformat()} | {event_id} | ERROR | {exc}", flush=True)

    paper_now = datetime.now(timezone.utc)
    executions = process_pending_orders(db, paper_now, cfg, policy.weather_fee_rate)
    settled = settle_positions(db, paper_now, cfg)
    for item in executions:
        if item["status"] == "NO_FILL":
            details = []
            if item.get("book_age_seconds") is not None:
                details.append(f"book_age={item['book_age_seconds']:.2f}s")
            if item.get("available_depth") is not None:
                details.append(f"depth={item['available_depth']:.2f}")
            extra = f" | {' | '.join(details)}" if details else ""
            print(
                f"PAPER NO FILL | {item['side']} | reason={item['reason']} | "
                f"signal_ask={item['signal_ask']:.3f} | {item.get('bracket')}{extra}",
                flush=True,
            )
            continue
        print(
            f"PAPER FILL | {item['side']} | shares={item['filled_shares']:.4f} | "
            f"vwap={item['vwap']:.4f} | cost={item['total_cost']:.2f} | "
            f"{item.get('bracket')}",
            flush=True,
        )
    for item in settled:
        print(
            f"PAPER SETTLED | {item['side']} | {item['status']} | "
            f"pnl={item['pnl']:+.2f}",
            flush=True,
        )
    summary["paper"] = paper_stats(db)
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
    db.executescript(PAPER_DDL)

    print(
        "LIVE PAPER SIGNALS | no orders are placed | "
        f"interval={interval:g}s | events={'all active' if requested_events is None else ','.join(requested_events)}",
        flush=True,
    )

    try:
        while True:
            cycle_started = time.monotonic()
            summary = run_cycle(db, policy, requested_events, None, cfg)
            print(
                f"CYCLE | events={summary['events']} TRADE={summary['trade']} "
                f"WATCH={summary['watch']} SKIP={summary['skip']} "
                f"errors={summary['errors']} new={len(summary['new_trades'])} | "
                f"PAPER pending={summary['paper']['pending']} "
                f"open={summary['paper']['filled_open']} "
                f"settled={summary['paper']['settled']} "
                f"realized_pnl={summary['paper']['realized_pnl']:+.2f}",
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
