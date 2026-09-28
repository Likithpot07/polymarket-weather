"""Depth-aware local paper execution and settlement accounting. Never places orders."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3

from collector import GAMMA, get_json
from edge import book_history, quoted_book
from probability import parse_time

PAPER_DDL = """
CREATE TABLE IF NOT EXISTS paper_orders (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    token_id TEXT NOT NULL,
    bracket TEXT,
    signal_at TEXT NOT NULL,
    execute_after TEXT NOT NULL,
    status TEXT NOT NULL,
    model_probability REAL NOT NULL,
    signal_ask REAL NOT NULL,
    desired_shares REAL NOT NULL,
    max_price REAL NOT NULL,
    filled_shares REAL NOT NULL DEFAULT 0,
    vwap REAL,
    fees REAL NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    fill_at TEXT,
    book_age_seconds REAL,
    available_depth REAL,
    yes_result INTEGER,
    payout REAL,
    pnl REAL,
    settled_at TEXT,
    settlement_source TEXT,
    last_error TEXT,
    UNIQUE(event_id,market_id,side,signal_at)
);
CREATE INDEX IF NOT EXISTS paper_order_status_time
ON paper_orders(status,execute_after);
CREATE INDEX IF NOT EXISTS paper_order_event_market
ON paper_orders(event_id,market_id,side);

CREATE TABLE IF NOT EXISTS paper_settlement_checks (
    event_id TEXT PRIMARY KEY,
    last_checked_at TEXT NOT NULL,
    status TEXT NOT NULL,
    last_error TEXT
);
"""


def paper_config(cfg: dict) -> dict:
    raw = cfg.get("paper", {})
    result = {
        "enabled": bool(raw.get("enabled", True)),
        "notional_usd": float(raw.get("notional_usd", 10.0)),
        "latency_ms": float(raw.get("latency_ms", 1000.0)),
        "max_slippage_per_share": float(raw.get("max_slippage_per_share", 0.03)),
        "max_book_age_seconds": float(raw.get("max_book_age_seconds", 10.0)),
        "settlement_check_seconds": float(raw.get("settlement_check_seconds", 300.0)),
        "max_trades_per_city_day": int(raw.get("max_trades_per_city_day", 3)),
        "deterioration_probability_drop": float(raw.get("deterioration_probability_drop", 0.15)),
        "deterioration_net_edge": float(raw.get("deterioration_net_edge", 0.0)),
    }
    if result["notional_usd"] <= 0:
        raise ValueError("paper.notional_usd must be positive")
    if result["latency_ms"] < 0:
        raise ValueError("paper.latency_ms cannot be negative")
    if not 0 <= result["max_slippage_per_share"] < 1:
        raise ValueError("paper.max_slippage_per_share must be between 0 and 1")
    if result["max_book_age_seconds"] <= 0 or result["settlement_check_seconds"] <= 0:
        raise ValueError("paper book/settlement intervals must be positive")
    if result["max_trades_per_city_day"] < 1:
        raise ValueError("paper.max_trades_per_city_day must be >= 1")
    if not 0 <= result["deterioration_probability_drop"] <= 1:
        raise ValueError("paper.deterioration_probability_drop must be between 0 and 1")
    if not math.isfinite(result["deterioration_net_edge"]):
        raise ValueError("paper.deterioration_net_edge must be finite")
    return result


def _finite_float(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def best_no_trade(decisions: list[dict]) -> dict | None:
    candidates = [
        decision for decision in decisions
        if decision.get("action") == "TRADE"
        and str(decision.get("side")).upper() == "NO"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda decision: (
            _finite_float(decision.get("net_edge"))
            if _finite_float(decision.get("net_edge")) is not None
            else float("-inf")
        ),
    )


def position_deteriorated(
    entry: dict,
    decisions: list[dict],
    probability_drop: float,
    net_edge_threshold: float,
) -> bool | None:
    current = next(
        (
            decision for decision in decisions
            if str(decision.get("market_id")) == str(entry.get("market_id"))
            and str(decision.get("side")).upper() == str(entry.get("side")).upper()
        ),
        None,
    )
    if current is None:
        return None
    entry_probability = _finite_float(entry.get("model_probability"))
    current_probability = _finite_float(current.get("model_probability"))
    current_edge = _finite_float(current.get("net_edge"))
    probability_bad = (
        entry_probability is not None
        and current_probability is not None
        and current_probability <= entry_probability - probability_drop
    )
    edge_bad = current_edge is not None and current_edge <= net_edge_threshold
    return probability_bad or edge_bad


def city_day_gate(
    db: sqlite3.Connection,
    city: str,
    target_date: str,
    decisions: list[dict],
    cfg: dict,
) -> tuple[bool, str]:
    """Allow one fill per city/day; reopen only when the latest fill deteriorates."""
    pcfg = paper_config(cfg)
    pending = db.execute(
        """SELECT count(*)
           FROM paper_orders p
           JOIN markets m ON m.id=p.market_id
           WHERE m.city=? AND m.target_date=? AND p.status='PENDING'""",
        (city, target_date),
    ).fetchone()[0]
    if pending:
        return False, "CITY_DAY_PENDING_ORDER"

    filled = db.execute(
        """SELECT count(*)
           FROM paper_orders p
           JOIN markets m ON m.id=p.market_id
           WHERE m.city=? AND m.target_date=?
             AND p.filled_shares>0""",
        (city, target_date),
    ).fetchone()[0]
    if filled == 0:
        return True, "FIRST_CITY_DAY_TRADE"
    if filled >= pcfg["max_trades_per_city_day"]:
        return False, "CITY_DAY_MAX_TRADES"

    latest = db.execute(
        """SELECT p.market_id,p.side,p.model_probability
           FROM paper_orders p
           JOIN markets m ON m.id=p.market_id
           WHERE m.city=? AND m.target_date=? AND p.filled_shares>0
           ORDER BY p.fill_at DESC,p.id DESC
           LIMIT 1""",
        (city, target_date),
    ).fetchone()
    if not latest:
        return False, "CITY_DAY_LOCKED"

    market_id, side, entry_probability = latest
    deteriorated = position_deteriorated(
        {
            "market_id": market_id,
            "side": side,
            "model_probability": entry_probability,
        },
        decisions,
        pcfg["deterioration_probability_drop"],
        pcfg["deterioration_net_edge"],
    )
    if deteriorated is None:
        return False, "LATEST_POSITION_NOT_EVALUATED"
    if deteriorated:
        return True, "LATEST_POSITION_DETERIORATED"
    return False, "CITY_DAY_HEALTHY_POSITION"


def schedule_order(
    db: sqlite3.Connection,
    state: dict,
    decision: dict,
    signal_at: datetime,
    cfg: dict,
) -> int | None:
    pcfg = paper_config(cfg)
    if (
        not pcfg["enabled"]
        or decision.get("action") != "TRADE"
        or str(decision.get("side")).upper() != "NO"
    ):
        return None
    token = decision.get("token_id")
    ask = decision.get("best_ask")
    probability = decision.get("model_probability")
    if not token or ask is None or probability is None:
        return None
    ask = float(ask)
    if not 0 < ask < 1:
        return None
    desired_shares = pcfg["notional_usd"] / ask
    max_price = min(0.999, ask + pcfg["max_slippage_per_share"])
    execute_after = signal_at + timedelta(milliseconds=pcfg["latency_ms"])
    cursor = db.execute(
        """INSERT OR IGNORE INTO paper_orders(
               event_id,market_id,side,token_id,bracket,signal_at,execute_after,status,
               model_probability,signal_ask,desired_shares,max_price
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            state["event_id"],
            decision["market_id"],
            decision["side"],
            str(token),
            decision.get("bracket"),
            signal_at.isoformat(),
            execute_after.isoformat(),
            "PENDING",
            float(probability),
            ask,
            desired_shares,
            max_price,
        ),
    )
    return cursor.lastrowid if cursor.rowcount else None


def simulate_buy_fill(
    ask_levels: list[dict],
    desired_shares: float,
    max_price: float,
    fee_rate: float,
) -> dict:
    remaining = float(desired_shares)
    fills = []
    available_depth = 0.0
    for level in sorted(ask_levels or [], key=lambda x: float(x["price"])):
        price = float(level["price"])
        size = float(level["size"])
        if price > max_price:
            break
        if size <= 0:
            continue
        available_depth += size
        take = min(size, remaining)
        if take <= 0:
            break
        fills.append((price, take))
        remaining -= take
        if remaining <= 1e-12:
            break
    filled = sum(size for _, size in fills)
    notional = sum(price * size for price, size in fills)
    fees = sum(fee_rate * price * (1 - price) * size for price, size in fills)
    vwap = notional / filled if filled else None
    return {
        "filled_shares": filled,
        "remaining_shares": max(0.0, desired_shares - filled),
        "vwap": vwap,
        "fees": fees,
        "total_cost": notional + fees,
        "available_depth": available_depth,
        "fills": fills,
    }


def process_pending_orders(
    db: sqlite3.Connection,
    as_of: datetime,
    cfg: dict,
    fee_rate: float,
) -> list[dict]:
    pcfg = paper_config(cfg)
    if not pcfg["enabled"]:
        return []
    rows = db.execute(
        """SELECT id,token_id,execute_after,desired_shares,max_price,event_id,
                  market_id,side,bracket,signal_ask
           FROM paper_orders
           WHERE status='PENDING' AND execute_after<=?
           ORDER BY execute_after,id""",
        (as_of.isoformat(),),
    ).fetchall()
    results_out = []
    for row in rows:
        (order_id, token, execute_after, desired, max_price, event_id,
         market_id, side, bracket, signal_ask) = row
        execution_time = parse_time(str(execute_after))
        history = book_history(db, {str(token)}).get(str(token), [])
        quote = quoted_book(history, execution_time, pcfg["max_book_age_seconds"])
        if not quote or not quote.get("depth_verified") or not quote.get("ask_levels"):
            reason = "NO_VERIFIED_BOOK_AT_EXECUTION"
            db.execute(
                """UPDATE paper_orders
                   SET status='NO_FILL',last_error=?,fill_at=?
                   WHERE id=?""",
                (reason, execution_time.isoformat(), order_id),
            )
            results_out.append({
                "order_id": order_id,
                "event_id": str(event_id),
                "market_id": str(market_id),
                "side": str(side),
                "bracket": bracket,
                "signal_ask": float(signal_ask),
                "status": "NO_FILL",
                "reason": reason,
            })
            continue
        result = simulate_buy_fill(
            quote["ask_levels"],
            float(desired),
            float(max_price),
            fee_rate,
        )
        filled = result["filled_shares"]
        if filled <= 0:
            reason = "PRICE_MOVED_BEYOND_SLIPPAGE_LIMIT_OR_NO_DEPTH"
            db.execute(
                """UPDATE paper_orders
                   SET status='NO_FILL',last_error=?,fill_at=?,book_age_seconds=?,
                       available_depth=?
                   WHERE id=?""",
                (
                    reason,
                    execution_time.isoformat(),
                    quote.get("book_age_seconds"),
                    result["available_depth"],
                    order_id,
                ),
            )
            results_out.append({
                "order_id": order_id,
                "event_id": str(event_id),
                "market_id": str(market_id),
                "side": str(side),
                "bracket": bracket,
                "signal_ask": float(signal_ask),
                "status": "NO_FILL",
                "reason": reason,
                "book_age_seconds": quote.get("book_age_seconds"),
                "available_depth": result["available_depth"],
            })
            continue
        status = "FILLED" if result["remaining_shares"] <= 1e-9 else "PARTIAL"
        db.execute(
            """UPDATE paper_orders
               SET status=?,filled_shares=?,vwap=?,fees=?,total_cost=?,fill_at=?,
                   book_age_seconds=?,available_depth=?,last_error=NULL
               WHERE id=?""",
            (
                status,
                result["filled_shares"],
                result["vwap"],
                result["fees"],
                result["total_cost"],
                execution_time.isoformat(),
                quote.get("book_age_seconds"),
                result["available_depth"],
                order_id,
            ),
        )
        results_out.append({
            "order_id": order_id,
            "event_id": str(event_id),
            "market_id": str(market_id),
            "side": str(side),
            "bracket": bracket,
            "signal_ask": float(signal_ask),
            **result,
            "status": status,
        })
    db.commit()
    return results_out


def settled_market_results(event: dict) -> dict[str, int]:
    closed = event.get("closed")
    if not (closed is True or str(closed).strip().lower() == "true"):
        return {}
    results = {}
    for market in event.get("markets") or []:
        outcomes = market.get("outcomes")
        prices = market.get("outcomePrices")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except json.JSONDecodeError:
                outcomes = []
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except json.JSONDecodeError:
                prices = []
        if [str(x).lower() for x in (outcomes or [])] != ["yes", "no"]:
            continue
        try:
            numeric = [float(x) for x in prices]
        except (TypeError, ValueError):
            continue
        if len(numeric) != 2 or max(numeric) < 0.99:
            continue
        results[str(market["id"])] = 1 if numeric[0] >= 0.99 else 0
    return results


def _settlement_due(db: sqlite3.Connection, event_id: str, as_of: datetime, seconds: float) -> bool:
    row = db.execute(
        "SELECT last_checked_at FROM paper_settlement_checks WHERE event_id=?",
        (event_id,),
    ).fetchone()
    if not row:
        return True
    try:
        return (as_of - parse_time(row[0])).total_seconds() >= seconds
    except ValueError:
        return True


def settle_positions(db: sqlite3.Connection, as_of: datetime, cfg: dict) -> list[dict]:
    pcfg = paper_config(cfg)
    if not pcfg["enabled"]:
        return []
    event_ids = [
        str(row[0]) for row in db.execute(
            """SELECT DISTINCT event_id FROM paper_orders
               WHERE status IN ('FILLED','PARTIAL')
                 AND settled_at IS NULL"""
        )
    ]
    settled = []
    for event_id in event_ids:
        if not _settlement_due(db, event_id, as_of, pcfg["settlement_check_seconds"]):
            continue
        try:
            event = get_json(f"{GAMMA}/events/{event_id}")
            if not isinstance(event, dict):
                raise ValueError("Gamma returned a non-object event")
            results = settled_market_results(event)
            check_status = "RESOLVED" if results else "PENDING"
            db.execute(
                """INSERT INTO paper_settlement_checks(event_id,last_checked_at,status,last_error)
                   VALUES (?,?,?,NULL)
                   ON CONFLICT(event_id) DO UPDATE SET
                       last_checked_at=excluded.last_checked_at,
                       status=excluded.status,
                       last_error=NULL""",
                (event_id, as_of.isoformat(), check_status),
            )
            if not results:
                continue
            rows = db.execute(
                """SELECT id,market_id,side,filled_shares,total_cost
                   FROM paper_orders
                   WHERE event_id=? AND status IN ('FILLED','PARTIAL')
                     AND settled_at IS NULL""",
                (event_id,),
            ).fetchall()
            for order_id, market_id, side, shares, total_cost in rows:
                if str(market_id) not in results:
                    continue
                yes_result = results[str(market_id)]
                won = (str(side).upper() == "YES" and yes_result == 1) or (
                    str(side).upper() == "NO" and yes_result == 0
                )
                payout = float(shares) if won else 0.0
                pnl = payout - float(total_cost)
                status = "SETTLED_WIN" if won else "SETTLED_LOSS"
                db.execute(
                    """UPDATE paper_orders
                       SET status=?,yes_result=?,payout=?,pnl=?,settled_at=?,
                           settlement_source=?
                       WHERE id=?""",
                    (
                        status,
                        yes_result,
                        payout,
                        pnl,
                        as_of.isoformat(),
                        f"{GAMMA}/events/{event_id}",
                        order_id,
                    ),
                )
                settled.append({
                    "order_id": order_id,
                    "event_id": event_id,
                    "market_id": str(market_id),
                    "side": str(side),
                    "status": status,
                    "payout": payout,
                    "pnl": pnl,
                })
        except Exception as exc:
            db.execute(
                """INSERT INTO paper_settlement_checks(event_id,last_checked_at,status,last_error)
                   VALUES (?,?,?,?)
                   ON CONFLICT(event_id) DO UPDATE SET
                       last_checked_at=excluded.last_checked_at,
                       status=excluded.status,
                       last_error=excluded.last_error""",
                (event_id, as_of.isoformat(), "ERROR", str(exc)),
            )
    db.commit()
    return settled


def stats(db: sqlite3.Connection) -> dict:
    counts = defaultdict(int)
    for status, count in db.execute("SELECT status,count(*) FROM paper_orders GROUP BY status"):
        counts[str(status)] = int(count)
    realized_pnl, settled_cost = db.execute(
        """SELECT COALESCE(sum(pnl),0),COALESCE(sum(total_cost),0)
           FROM paper_orders WHERE settled_at IS NOT NULL"""
    ).fetchone()
    open_cost = db.execute(
        """SELECT COALESCE(sum(total_cost),0) FROM paper_orders
           WHERE status IN ('FILLED','PARTIAL') AND settled_at IS NULL"""
    ).fetchone()[0]
    return {
        "pending": counts["PENDING"],
        "filled_open": counts["FILLED"] + counts["PARTIAL"],
        "no_fill": counts["NO_FILL"],
        "settled": counts["SETTLED_WIN"] + counts["SETTLED_LOSS"],
        "wins": counts["SETTLED_WIN"],
        "losses": counts["SETTLED_LOSS"],
        "realized_pnl": float(realized_pnl or 0),
        "settled_cost": float(settled_cost or 0),
        "open_cost": float(open_cost or 0),
        "realized_return_pct": (
            100.0 * float(realized_pnl) / float(settled_cost)
            if settled_cost else None
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("command", choices=("status", "settle"))
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
    with sqlite3.connect(db_path, timeout=30) as db:
        db.execute("PRAGMA busy_timeout=30000")
        db.executescript(PAPER_DDL)
        if args.command == "settle":
            settled = settle_positions(db, datetime.now(timezone.utc), cfg)
            print(json.dumps({"settled_now": settled, "stats": stats(db)}, indent=2))
        else:
            print(json.dumps(stats(db), indent=2))


if __name__ == "__main__":
    main()
