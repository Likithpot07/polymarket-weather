import sqlite3
db = sqlite3.connect("data/weather.sqlite3")
for oid in (36, 48, 13):  # pick a few losing order ids
    o = db.execute("SELECT market_id, token_id, side, bracket FROM paper_orders WHERE id=?", (oid,)).fetchone()
    market_id, token_id, side, bracket = o
    t = db.execute("SELECT market_id, side FROM tokens WHERE token_id=?", (token_id,)).fetchone()
    q = db.execute("SELECT question FROM markets WHERE id=?", (market_id,)).fetchone()
    print(f"order {oid}: paper_orders.bracket={bracket!r}")
    print(f"           tokens row matches market_id? {t[0]==market_id}  side matches? {t[1]==side}")
    print(f"           markets.question={q[0]!r}")