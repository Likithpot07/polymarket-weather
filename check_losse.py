import sqlite3
import json

db = sqlite3.connect("data/weather.sqlite3")

losses = db.execute(
    """
    SELECT id,event_id,market_id,bracket,side,model_probability,
           signal_ask,signal_at,pnl
    FROM paper_orders
    WHERE status='SETTLED_LOSS'
    ORDER BY settled_at
    """
).fetchall()

for row in losses:
    oid, eid, mid, bracket, side, p, ask, signal_at, pnl = row

    run = db.execute(
        """
        SELECT id,as_of,model_version,diagnostics_json
        FROM probability_runs
        WHERE event_id=? AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
        """,
        (eid, signal_at),
    ).fetchone()

    print("\n==============================")
    print("LOSS", oid, "| EVENT", eid)
    print("BRACKET:", bracket)
    print("ENTRY:", signal_at)
    print("SIDE:", side)
    print("MODEL P:", round(p, 4))
    print("ASK:", ask)
    print("PNL:", round(pnl, 2))

    if not run:
        print("NO PROBABILITY RUN FOUND")
        continue

    run_id, as_of, version, diagnostics_json = run
    diagnostics = json.loads(diagnostics_json)

    print("MODEL:", version)
    print("MODEL RUN:", as_of)
    print("OBSERVED:", diagnostics.get("in_day_observation"))

    print("COMPONENTS:")
    for component in diagnostics.get("components", []):
        print(component)

    bracket_prob = db.execute(
        """
        SELECT p_yes,p_no
        FROM bracket_probabilities
        WHERE run_id=? AND market_id=?
        """,
        (run_id, mid),
    ).fetchone()

    print("BRACKET_PROB:", bracket_prob)

db.close()