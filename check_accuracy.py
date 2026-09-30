import sqlite3

db = sqlite3.connect("data/weather.sqlite3")

rows = db.execute(
    """
    SELECT status, COUNT(*), COALESCE(SUM(pnl),0)
    FROM paper_orders
    WHERE signal_at >= ?
      AND status IN ('SETTLED_WIN','SETTLED_LOSS')
    GROUP BY status
    """,
    ("2026-09-29T08:13:04+00:00",),
).fetchall()

wins = sum(count for status, count, _ in rows if status == "SETTLED_WIN")
losses = sum(count for status, count, _ in rows if status == "SETTLED_LOSS")
pnl = sum(value for _, _, value in rows)
settled = wins + losses

print("settled =", settled)
print("wins =", wins)
print("losses =", losses)
print("accuracy =", round(wins / settled * 100, 2) if settled else "N/A")
print("pnl =", round(pnl, 2))

db.close()