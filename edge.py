"""Shared read-only CLOB quote and executable buy-edge calculations."""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
import json
import math
import sqlite3

from probability import parse_time


def book_history(db: sqlite3.Connection, tokens: set[str]) -> dict[str, tuple[list[datetime], list[dict], list[datetime], list[datetime], list[tuple[float, float]]]]:
    """Full books, later updates, and separately recorded top-of-book quotes."""
    history = {
        token: ([], [], [], [], []) for token in tokens
    }
    if not tokens:
        return history
    for token, received, kind, raw, bid, ask in db.execute("""SELECT token_id,received_at,event_type,raw_json,best_bid,best_ask
        FROM book_events WHERE event_type IN ('book','price_change','best_bid_ask','last_trade_price')
        ORDER BY received_at,id"""):
        if token not in history:
            continue
        try:
            when = parse_time(received)
            if kind != "book":
                history[token][2].append(when)
                if kind in ("price_change", "best_bid_ask") and bid is not None and ask is not None:
                    top_bid, top_ask = float(bid), float(ask)
                    if all(math.isfinite(v) for v in (top_bid, top_ask)) and 0 <= top_bid < top_ask <= 1:
                        history[token][3].append(when)
                        history[token][4].append((top_bid, top_ask))
                continue
            frame = json.loads(raw)
            if not isinstance(frame.get("asks"), list) or not isinstance(frame.get("bids"), list):
                continue
            history[token][0].append(when)
            history[token][1].append(frame)
        except (ValueError, TypeError, KeyError):
            continue
    return history


def quoted_book(history: tuple[list[datetime], list[dict], list[datetime], list[datetime], list[tuple[float, float]]],
                as_of: datetime, max_age: float) -> dict | None:
    times, frames, updates, top_times, top_quotes = history
    pos = bisect_right(times, as_of) - 1
    update_pos = bisect_right(updates, as_of) - 1
    if pos >= 0:
        age = (as_of - times[pos]).total_seconds()
        if 0 <= age <= max_age and (update_pos < 0 or updates[update_pos] <= times[pos]):
            try:
                bids = [(float(x["price"]), float(x["size"])) for x in frames[pos]["bids"]]
                asks = [(float(x["price"]), float(x["size"])) for x in frames[pos]["asks"]]
                if bids and asks:
                    bid = max(p for p, _ in bids)
                    ask = min(p for p, _ in asks)
                    depth = sum(s for p, s in asks if p == ask)
                    if all(math.isfinite(x) for x in (bid, ask, depth)) and 0 <= bid < ask <= 1 and depth > 0:
                        return {"best_bid": bid, "best_ask": ask, "ask_size": depth,
                                "depth_verified": True, "quote_source": "book",
                                "book_age_seconds": age, "received_at": times[pos].isoformat()}
            except (ValueError, TypeError, KeyError):
                pass
    # A newer price update has a usable ask but cannot prove remaining depth.
    top_pos = bisect_right(top_times, as_of) - 1
    if top_pos < 0:
        return None
    age = (as_of - top_times[top_pos]).total_seconds()
    if age < 0 or age > max_age or (update_pos >= 0 and updates[update_pos] > top_times[top_pos]):
        return None
    bid, ask = top_quotes[top_pos]
    return {"best_bid": bid, "best_ask": ask, "ask_size": None,
            "depth_verified": False, "quote_source": "top_of_book",
            "book_age_seconds": age, "received_at": top_times[top_pos].isoformat()}


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
