import sqlite3
from datetime import datetime
from edge import book_history, quoted_book

db = sqlite3.connect("data/weather.sqlite3")

rows = db.execute("""
SELECT
    id,
    event_id,
    market_id,
    token_id,
    execute_after,
    signal_ask,
    last_error
FROM paper_orders
WHERE status='NO_FILL'
ORDER BY id
""").fetchall()

for row in rows:
    order_id, event_id, market_id, token_id, execute_after, signal_ask, last_error = row

    history = book_history(db, {str(token_id)}).get(str(token_id), [])
    execution_time = datetime.fromisoformat(execute_after)

    before = [
        e for e in history
        if e["received_at"] <= execution_time
    ]

    latest = before[-1] if before else None
    latest_book = next(
        (e for e in reversed(before) if e["event_type"] == "book"),
        None
    )

    quote = quoted_book(history, execution_time, 10.0)

    print("\n==============================")
    print("ORDER:", order_id)
    print("EVENT:", event_id)
    print("MARKET:", market_id)
    print("SIGNAL ASK:", signal_ask)
    print("EXECUTION:", execute_after)
    print("BOOK EVENTS BEFORE EXECUTION:", len(before))

    if latest:
        age = (execution_time - latest["received_at"]).total_seconds()
        print("LATEST EVENT:", latest["event_type"])
        print("LATEST EVENT AGE:", round(age, 3), "sec")
    else:
        print("LATEST EVENT: NONE")

    if latest_book:
        age = (execution_time - latest_book["received_at"]).total_seconds()
        print("LATEST FULL BOOK AGE:", round(age, 3), "sec")
    else:
        print("LATEST FULL BOOK: NONE")

    print("QUOTED BOOK:", quote)

db.close()