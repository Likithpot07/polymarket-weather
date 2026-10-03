"""Shared read-only CLOB quote and executable buy-edge calculations."""
from __future__ import annotations

from datetime import datetime
import json
import math
import sqlite3

from probability import parse_time


def _finite_number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_top(bid, ask) -> tuple[float, float] | None:
    best_bid = _finite_number(bid)
    best_ask = _finite_number(ask)
    if best_bid is None or best_ask is None:
        return None
    if not (0 <= best_bid < best_ask <= 1):
        return None
    return best_bid, best_ask


def _levels(raw_levels) -> dict[float, float]:
    result: dict[float, float] = {}
    if not isinstance(raw_levels, list):
        return result
    for level in raw_levels:
        if not isinstance(level, dict):
            continue
        price = _finite_number(level.get("price"))
        size = _finite_number(level.get("size"))
        if price is None or size is None:
            continue
        if 0 <= price <= 1 and size > 0:
            result[price] = size
    return result


def _row_event(row_id, token, received, kind, raw, bid, ask) -> dict | None:
    try:
        when = parse_time(received)
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            payload = {}
    except (ValueError, TypeError, json.JSONDecodeError):
        return None
    return {"id": int(row_id), "token_id": str(token), "received_at": when, "event_type": str(kind),
            "raw": payload, "best_bid": bid, "best_ask": ask}


def recent_book_history(db: sqlite3.Connection, tokens: set[str], as_of: datetime,
                        fallback_hours: float = 3.0) -> dict[str, list[dict]]:
    """Only what quoted_book needs: from the latest full snapshot at/before as_of up to as_of.

    Loading a token's whole history (the old way) made live.py take an hour per cycle
    on a large database.
    """
    from datetime import timedelta
    history: dict[str, list[dict]] = {token: [] for token in tokens}
    end = as_of.isoformat()
    for token in tokens:
        anchor = db.execute(
            """SELECT received_at FROM book_events
               WHERE token_id=? AND event_type='book' AND received_at<=?
               ORDER BY received_at DESC LIMIT 1""", (token, end)).fetchone()
        start = anchor[0] if anchor else (as_of - timedelta(hours=fallback_hours)).isoformat()
        for row in db.execute(
                """SELECT id,token_id,received_at,event_type,raw_json,best_bid,best_ask FROM book_events
                   WHERE token_id=? AND received_at>=? AND received_at<=?
                     AND event_type IN ('book','price_change','best_bid_ask')
                   ORDER BY received_at,id""", (token, start, end)):
            event = _row_event(*row)
            if event:
                history[token].append(event)
    return history


def book_history(db: sqlite3.Connection, tokens: set[str]) -> dict[str, list[dict]]:
    """Load only the requested tokens, preserving snapshots and incremental book changes."""
    history: dict[str, list[dict]] = {token: [] for token in tokens}
    if not tokens:
        return history
    placeholders = ",".join("?" for _ in tokens)
    query = f"""SELECT id,token_id,received_at,event_type,raw_json,best_bid,best_ask
        FROM book_events
        WHERE token_id IN ({placeholders})
          AND event_type IN ('book','price_change','best_bid_ask')
        ORDER BY received_at,id"""
    for row_id, token, received, kind, raw, bid, ask in db.execute(query, tuple(tokens)):
        token = str(token)
        if token not in history:
            continue
        try:
            when = parse_time(received)
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                payload = {}
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        history[token].append({
            "id": int(row_id),
            "token_id": token,
            "received_at": when,
            "event_type": str(kind),
            "raw": payload,
            "best_bid": bid,
            "best_ask": ask,
        })
    return history


def quoted_book(history: list[dict], as_of: datetime, max_age: float) -> dict | None:
    """Reconstruct the latest L2 book by replaying price_change deltas after a full snapshot."""
    bids: dict[float, float] | None = None
    asks: dict[float, float] | None = None
    token_id: str | None = None
    last_depth_at: datetime | None = None
    replayed_change = False
    latest_top: tuple[datetime, float, float] | None = None

    for event in history:
        when = event["received_at"]
        if when > as_of:
            break
        token_id = event["token_id"]
        kind = event["event_type"]
        raw = event["raw"]

        top = _valid_top(event.get("best_bid"), event.get("best_ask"))
        if top is not None and kind in ("price_change", "best_bid_ask"):
            latest_top = (when, top[0], top[1])

        if kind == "book":
            new_bids = _levels(raw.get("bids"))
            new_asks = _levels(raw.get("asks"))
            bids, asks = new_bids, new_asks
            last_depth_at = when
            replayed_change = False
            continue

        if kind != "price_change" or bids is None or asks is None:
            continue

        changes = raw.get("price_changes")
        if not isinstance(changes, list):
            changes = [raw]

        applied = False
        for change in changes:
            if not isinstance(change, dict):
                continue
            change_token = str(change.get("asset_id") or token_id or "")
            if token_id and change_token != token_id:
                continue
            price = _finite_number(change.get("price"))
            size = _finite_number(change.get("size"))
            side = str(change.get("side") or "").upper()
            if price is None or size is None or not (0 <= price <= 1) or size < 0:
                continue
            levels = bids if side == "BUY" else asks if side == "SELL" else None
            if levels is None:
                continue
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            applied = True

            change_top = _valid_top(change.get("best_bid"), change.get("best_ask"))
            if change_top is not None:
                latest_top = (when, change_top[0], change_top[1])

        if applied:
            last_depth_at = when
            replayed_change = True

    if bids is not None and asks is not None and bids and asks and last_depth_at is not None:
        best_bid = max(bids)
        best_ask = min(asks)
        ask_size = asks[best_ask]
        valid = _valid_top(best_bid, best_ask)
        if valid is not None and ask_size > 0 and math.isfinite(ask_size):
            inconsistent_top = (
                latest_top is not None
                and latest_top[0] > last_depth_at
                and (
                    not math.isclose(latest_top[1], best_bid, abs_tol=1e-12)
                    or not math.isclose(latest_top[2], best_ask, abs_tol=1e-12)
                )
            )
            age = (as_of - last_depth_at).total_seconds()
            if not inconsistent_top and 0 <= age <= max_age:
                return {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "ask_size": ask_size,
                    "bid_levels": [{"price": price, "size": bids[price]} for price in sorted(bids, reverse=True)],
                    "ask_levels": [{"price": price, "size": asks[price]} for price in sorted(asks)],
                    "depth_verified": True,
                    "quote_source": "reconstructed_book" if replayed_change else "book",
                    "book_age_seconds": age,
                    "received_at": last_depth_at.isoformat(),
                }

    if latest_top is None:
        return None
    top_time, best_bid, best_ask = latest_top
    age = (as_of - top_time).total_seconds()
    if age < 0 or age > max_age:
        return None
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "ask_size": None,
        "bid_levels": None,
        "ask_levels": None,
        "depth_verified": False,
        "quote_source": "top_of_book",
        "book_age_seconds": age,
        "received_at": top_time.isoformat(),
    }


def buy_edge(probability: float, ask: float, weather_fee_rate: float = 0.05,
             slippage_per_share: float = 0.01) -> dict:
    """Per-share estimated edge for crossing the ask and holding to resolution.

    Polymarket weather taker fee formula: shares * fee_rate * price * (1 - price).
    This assumes one share and a taker fill; it does not simulate execution.
    """
    values = (probability, ask, weather_fee_rate, slippage_per_share)
    if not all(math.isfinite(v) for v in values) or not (0 <= probability <= 1 and 0 < ask < 1):
        raise ValueError("invalid model probability or ask")
    if weather_fee_rate < 0 or slippage_per_share < 0:
        raise ValueError("fee and slippage must be nonnegative")
    fee = weather_fee_rate * ask * (1 - ask)
    cost = ask + fee + slippage_per_share
    return {"raw_edge": probability - ask, "fee_per_share": fee,
            "slippage_per_share": slippage_per_share,
            "entry_cost": cost, "net_edge": probability - cost}
