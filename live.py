"""Continuous paper/live signal runner. Reads collector data; never places orders."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

import os
import re
import subprocess
import sys
import threading
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from decide import event_decisions
from filters import Policy
from paper import PAPER_DDL, tag_legacy_orders, best_no_trade, city_day_gate, locked_candidates, process_pending_orders, schedule_order, settle_positions, stats as paper_stats

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

CREATE TABLE IF NOT EXISTS live_model_diagnostics (
    run_id INTEGER PRIMARY KEY REFERENCES live_decision_runs(id),
    event_id TEXT NOT NULL,
    as_of TEXT NOT NULL,
    model_version TEXT,
    diagnostics_json TEXT NOT NULL
);

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


_LAST_FULL_STORE: dict[str, datetime] = {}  # event_id -> last time full detail was saved


def persist_snapshot(
    db: sqlite3.Connection,
    state: dict,
    decisions: list[dict],
    as_of: datetime,
    store_full: bool = True,
) -> list[dict]:
    """Persist one evaluation and return only newly-entered TRADE signals.

    store_full=False skips the bulky per-bracket rows (live_decisions, diagnostics);
    signal state and paper trades are always kept. Keeps the database small.
    """
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
    if store_full:
        db.execute(
        """INSERT INTO live_model_diagnostics(
               run_id,event_id,as_of,model_version,diagnostics_json
           ) VALUES (?,?,?,?,?)""",
        (
            run_id,
            state["event_id"],
            as_of.isoformat(),
            state.get("model_version"),
            json.dumps(
                {
                    "status": state.get("status"),
                    "notes": state.get("notes", []),
                    "components": state.get("components", []),
                    "in_day_observation": state.get("in_day_observation"),
                    "in_day_hourly_context": state.get("in_day_hourly_context", {}),
                    "in_day_observations_modeled": state.get("in_day_observations_modeled"),
                    "combined": state.get("combined"),
                },
                separators=(",", ":"),
            ),
        ),
    )
    new_trades: list[dict] = []

    for decision in decisions:
        if store_full:
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
    scheduled: list[dict],
    skip_reasons: dict[int, str],
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
    for decision in current_trades:
        selected = any(decision is t for t in scheduled)
        if selected:
            reason = "SELECTED_AND_SCHEDULED"
        else:
            reason = skip_reasons.get(id(decision), "PAPER_ORDER_NOT_SCHEDULED")
        if decision.get("tags"):
            reason += " [TAGS:" + ",".join(decision["tags"]) + "]"
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
    blended = decision.get("blended_probability")
    market = decision.get("market_probability")
    if blended is not None:
        probability_text += f" blend={blended * 100:.1f}%"
    if market is not None:
        probability_text += f" mkt={market * 100:.1f}%"
    if decision.get("locked_no"):
        probability_text += " LOCKED"
    if decision.get("tags"):
        probability_text += " TAGGED:" + ",".join(decision["tags"])
    return (
        f"{decision['side']} | p={probability_text} | ask={ask_text} | "
        f"depth={depth_text} | net_edge={edge_text} | {decision.get('bracket')}"
    )


def load_env(path: Path) -> None:
    """Read KEY=value lines from .env into os.environ (no extra package needed)."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def telegram(text: str, timeout: float = 10) -> None:
    """Send a Telegram message if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set. Never crashes the bot."""
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        data = urlencode({"chat_id": chat, "text": text}).encode()
        with urlopen(Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data), timeout=timeout) as r:
            r.read()
    except Exception as exc:  # network trouble must not stop trading
        print(f"TELEGRAM FAILED | {re.sub(r'bot[0-9]+:[A-Za-z0-9_-]+', 'bot***', str(exc))}", flush=True)


# --- Telegram commands: reply to /status, /open, /today, /why from YOUR chat only ---
# Fixed list of read-only scripts; nothing typed in Telegram is ever run as a command.
TELEGRAM_COMMANDS = {
    "/status": ["paper.py", "status"],
    "/open": ["open_bets.py"],
    "/today": ["open_bets.py", "--today"],
    "/why": ["why_no_trades.py"],
}
TELEGRAM_HELP = "Commands:\n/status - paper P&L\n/open - all open bets\n/today - bets settling today\n/why - health check"


def telegram_api(method: str, params: dict, timeout: float = 35) -> dict:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    data = urlencode(params).encode()
    with urlopen(Request(f"https://api.telegram.org/bot{token}/{method}", data=data), timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def run_script(args: list[str], workdir: Path) -> str:
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    try:
        out = subprocess.run(
            [sys.executable, *args], cwd=workdir, env=env, capture_output=True,
            timeout=120, text=True, encoding="utf-8", errors="replace",
        )
        text = (out.stdout or "") + (("\n" + out.stderr[-1500:]) if out.returncode else "")
    except subprocess.TimeoutExpired:
        text = "Timed out after 120s."
    return text.strip() or "(no output)"


def telegram_long(text: str) -> None:
    """Telegram caps messages at 4096 chars, so send long output in pieces."""
    for start in range(0, len(text), 3900):
        telegram(text[start:start + 3900])


def telegram_command_loop(workdir: Path) -> None:
    """Background thread: long-polls Telegram and answers commands. Never crashes the bot."""
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    offset = None
    try:  # skip messages sent while the bot was off
        old = telegram_api("getUpdates", {"offset": -1, "timeout": 0}).get("result", [])
        if old:
            offset = old[-1]["update_id"] + 1
    except Exception:
        pass
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": '["message"]'}
            if offset is not None:
                params["offset"] = offset
            for update in telegram_api("getUpdates", params).get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or {}
                if str(msg.get("chat", {}).get("id")) != chat:
                    continue  # ignore anyone who isn't you
                word = (msg.get("text") or "").strip().split(" ")[0].split("@")[0].lower()
                if word in TELEGRAM_COMMANDS:
                    telegram(f"Running {word} ...")
                    telegram_long(run_script(TELEGRAM_COMMANDS[word], workdir))
                elif word.startswith("/"):
                    telegram(TELEGRAM_HELP)
        except Exception as exc:
            print(f"TELEGRAM POLL FAILED | {re.sub(r'bot[0-9]+:[A-Za-z0-9_-]+', 'bot***', str(exc))}", flush=True)
            time.sleep(15)


def start_telegram_commands(workdir: Path) -> None:
    if os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        threading.Thread(target=telegram_command_loop, args=(workdir,), daemon=True).start()


def short_question(text: str | None) -> str:
    m = re.search(r"in (.+?) be (.+?)(?: on |\?|$)", text or "")
    return f"{m.group(1)}: {m.group(2)}" if m else (text or "")[:60]


TAG_NAMES = {"MODEL_DISAGREEMENT": "models disagree",
             "FORECAST_TOO_CLOSE_TO_BRACKET": "forecast close to bracket"}
TAGGING_START = "2026-10-08T08:00:00+00:00"  # bets before this were never tagged
ALERT_TZ = "Asia/Kolkata"  # times in Telegram alerts


def tag_line(db: sqlite3.Connection, order_id) -> str:
    """Telegram lines: when the bet was placed, its target day, and clean/tagged."""
    from zoneinfo import ZoneInfo
    try:
        row = db.execute(
            """SELECT p.tags,p.signal_at,m.target_date FROM paper_orders p
               LEFT JOIN markets m ON m.id=p.market_id WHERE p.id=?""", (order_id,)).fetchone()
    except sqlite3.Error:
        return ""
    if not row:
        return ""
    tags_text, signal_at, target = row
    lines = []
    try:
        placed = datetime.fromisoformat(str(signal_at)).astimezone(ZoneInfo(ALERT_TZ))
        lines.append(f"Placed {placed:%d %b %H:%M} IST" + (f" | for {target}" if target else ""))
    except (TypeError, ValueError):
        pass
    tags = [t for t in str(tags_text or "").split(",") if t]
    if tags:
        lines.append("🏷 TAGGED: " + ", ".join(TAG_NAMES.get(t, t) for t in tags))
    elif str(signal_at) < TAGGING_START:
        lines.append("⚪ placed before tagging started")
    else:
        lines.append("🟢 CLEAN (passed every rule)")
    return "".join("\n" + line for line in lines)


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
            scheduled: list[dict] = []
            skip_reasons: dict[int, str] = {}
            every = float(cfg.get("decision_store_minutes", 0))
            last_full = _LAST_FULL_STORE.get(event_id)
            store_full = (every <= 0 or last_full is None
                          or any(d["action"] == "TRADE" for d in decisions)
                          or (evaluation_time - last_full).total_seconds() >= every * 60)
            with db:
                new_trades = persist_snapshot(db, state, decisions, evaluation_time,
                                              store_full=store_full)
                if store_full:
                    _LAST_FULL_STORE[event_id] = evaluation_time
                locked = locked_candidates(db, state["event_id"], decisions)
                if locked:
                    # Locked brackets can't win; each is its own trade, so skip the city-day limit.
                    groups = [(locked, True, "LOCKED_NO", False)]
                else:
                    # Clean bets and tagged bets (shadow rule failed) each get their own slot.
                    groups = []
                    for is_tagged in (False, True):
                        group = [d for d in decisions if bool(d.get("tags")) == is_tagged]
                        allowed, reason = city_day_gate(
                            db, state["city"], state["target_date"], decisions, cfg,
                            tagged=is_tagged,
                        )
                        groups.append((group, allowed, reason, is_tagged))
                for group, allowed, reason, _ in groups:
                    best = best_no_trade(group)
                    if allowed and best:
                        order_id = schedule_order(
                            db, state, best, datetime.now(timezone.utc), cfg,
                        )
                        if order_id:
                            scheduled.append(best)
                    for d in group:
                        if d is best and any(d is t for t in scheduled):
                            continue
                        if best is not None and d is not best:
                            skip_reasons[id(d)] = "LOWER_EDGE_THAN_SELECTED_NO"
                        elif not allowed:
                            skip_reasons[id(d)] = reason or "CITY_DAY_GATE_BLOCKED"
                        else:
                            skip_reasons[id(d)] = "PAPER_ORDER_NOT_SCHEDULED"
                gate_reason = ",".join(r for _, _, r, _ in groups if r)
                persist_candidate_audit(
                    db,
                    state,
                    decisions,
                    evaluation_time,
                    scheduled,
                    skip_reasons,
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
            for trade in scheduled:
                print(
                    f"PAPER ORDER SCHEDULED | {format_trade(trade)} | "
                    f"gate={gate_reason}",
                    flush=True,
                )
            if current_trades:
                for decision in current_trades:
                    if any(decision is t for t in scheduled):
                        continue
                    reason = skip_reasons.get(id(decision), "PAPER_ORDER_NOT_SCHEDULED")
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
        telegram(f"🟦 NEW BET (paper)\nNO on {short_question(item.get('bracket'))}\n"
                 f"Paid ${item['total_cost']:.2f} at {item['vwap']:.2f} | win pays ${item['filled_shares']:.2f}"
                 f"{tag_line(db, item.get('order_id'))}")
    for item in settled:
        print(
            f"PAPER SETTLED | {item['side']} | {item['status']} | "
            f"pnl={item['pnl']:+.2f}",
            flush=True,
        )
        row = db.execute("SELECT bracket FROM paper_orders WHERE id=?", (item["order_id"],)).fetchone()
        won = item["status"] == "SETTLED_WIN"
        telegram(f"{'✅ WON' if won else '❌ LOST'} {item['pnl']:+.2f} USD (paper)\n"
                 f"NO on {short_question(row[0] if row else '')}"
                 f"{tag_line(db, item.get('order_id'))}")
    if settled:
        st = paper_stats(db)
        telegram(f"📊 New bot paper P&L: {st['realized_pnl']:+.2f} USD | "
                 f"{st['wins']} wins / {st['losses']} losses")
    summary["paper"] = paper_stats(db)
    return summary


class _StopSignal(Exception):
    """Raised when Windows/OS asks the bot to stop (window closed, shutdown, kill)."""


def _install_stop_handlers() -> None:
    import signal

    def handler(signum, _frame):
        raise _StopSignal(f"window closed or system stop (signal {signum})")
    for name in ("SIGTERM", "SIGBREAK"):  # SIGBREAK = console window closed on Windows
        if hasattr(signal, name):
            try:
                signal.signal(getattr(signal, name), handler)
            except (ValueError, OSError):
                pass


def main() -> None:
    """Runs the bot and always sends a Telegram message when it stops, whatever the reason."""
    once = "--once" in sys.argv
    reason = "stopped"
    _install_stop_handlers()
    try:
        _main()
        reason = "finished"
    except KeyboardInterrupt:
        reason = "stopped by you (Ctrl+C)"
        print("Stopped.", flush=True)
    except _StopSignal as exc:
        reason = str(exc)
    except SystemExit as exc:
        reason = f"exited (code {exc.code})"
        raise
    except BaseException as exc:
        detail = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot***", f"{type(exc).__name__}: {exc}")
        reason = f"CRASHED - {detail}"[:300]
        raise
    finally:
        if not once:
            telegram(f"🛑 Weather bot (live.py) STOPPED - {reason}\n"
                     "Open bets are safe; they settle once it runs again.", timeout=4)


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--event-id", action="append", help="Limit to event ID; repeatable")
    parser.add_argument("--interval", type=float, help="Seconds between decision cycles")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    load_env(config_path.parent / ".env")
    telegram("🤖 Weather bot (live.py) started - paper mode\nSend /help for commands")
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
    tag_legacy_orders(db)

    if not args.once:
        start_telegram_commands(config_path.parent)

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
    finally:
        db.close()


if __name__ == "__main__":
    main()
