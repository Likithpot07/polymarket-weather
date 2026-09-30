"""Step 3: evaluate YES and NO for one event without placing orders."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from edge import book_history, quoted_book
from filters import Policy, assess
from probability import evaluate, parse_time


def verified_at(flag: int, reviewed_at: str | None, as_of: datetime) -> bool:
    if flag != 1 or not reviewed_at:
        return False
    try:
        return parse_time(reviewed_at) <= as_of
    except ValueError:
        return False


def event_decisions(
    db: sqlite3.Connection,
    eid: str,
    as_of: datetime,
    policy: Policy,
    history: dict[str, list[dict]] | None = None,
) -> tuple[dict, list[dict]]:
    state = evaluate(
        db,
        eid,
        as_of,
        min_days=policy.min_calibration_days,
        persist=False,
    )

    markets = db.execute(
        """SELECT m.id,t.token_id,t.side,r.verified,r.reviewed_at
        FROM markets m
        JOIN tokens t ON t.market_id=m.id
        JOIN rule_reviews r ON r.market_id=m.id
        WHERE m.event_id=?""",
        (eid,),
    ).fetchall()

    mapping = {
        (mid, side): (token, verified_at(verified, reviewed_at, as_of))
        for mid, token, side, verified, reviewed_at in markets
    }

    if history is None:
        history = book_history(
            db,
            {token for _, token, _, _, _ in markets},
        )

    tz_row = db.execute(
        "SELECT timezone FROM locations WHERE city=?",
        (state["city"],),
    ).fetchone()

    if not tz_row:
        raise ValueError(f"Missing timezone for {state['city']}")

    decisions = []

    for outcome in state["probabilities"]:
        for side in ("YES", "NO"):
            key = (outcome["market_id"], side)
            token, verified = mapping.get(key, (None, False))

            quote = (
                quoted_book(
                    history.get(token, []),
                    as_of,
                    policy.max_book_age_seconds,
                )
                if token
                else None
            )

            decision = assess(
                state,
                outcome,
                side,
                quote,
                as_of,
                tz_row[0],
                verified,
                policy,
            )

            decision["token_id"] = token
            decision["bracket"] = outcome["bracket"]
            decisions.append(decision)

    priority = {"TRADE": 0, "WATCH": 1, "SKIP": 2}

    decisions.sort(
        key=lambda x: (
            priority[x["action"]],
            -(x["net_edge"] or -999),
        )
    )

    return state, decisions


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--event-id", required=True)
    ap.add_argument("--as-of", default=datetime.now(timezone.utc).isoformat())
    ap.add_argument("--show", type=int, default=5, help="Top decisions; 0 prints all")
    args = ap.parse_args()

    cfg_file = Path(args.config).resolve()
    cfg = json.loads(cfg_file.read_text())

    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else cfg_file.parent / db_path

    if not db_path.is_file():
        ap.error(f"Collector database missing: {db_path}")

    try:
        policy = Policy.from_config(cfg)
        as_of = parse_time(args.as_of)

        with sqlite3.connect(db_path) as db:
            state, decisions = event_decisions(
                db,
                args.event_id,
                as_of,
                policy,
            )

            latest = db.execute(
                """SELECT MAX(b.received_at)
                FROM book_events b
                JOIN tokens t ON t.token_id=b.token_id
                JOIN markets m ON m.id=t.market_id
                WHERE m.event_id=? AND b.received_at<=?""",
                (args.event_id, as_of.isoformat()),
            ).fetchone()[0]

    except (ValueError, sqlite3.Error) as exc:
        ap.error(str(exc))

    output = {
        "event_id": state["event_id"],
        "city": state["city"],
        "target_date": state["target_date"],
        "as_of": state["as_of"],
        "last_market_update_at": latest,
        "seconds_since_last_market_update": (
            round((as_of - parse_time(latest)).total_seconds(), 1)
            if latest
            else None
        ),
        "counts": dict(Counter(d["action"] for d in decisions)),
        "decisions": decisions[:args.show] if args.show else decisions,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()