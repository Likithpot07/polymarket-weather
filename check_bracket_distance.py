import sqlite3
import json
import re

CUTOFF = "2026-09-29T08:13:04+00:00"

db = sqlite3.connect("data/weather.sqlite3")
db.row_factory = sqlite3.Row

rows = db.execute("""
SELECT
    o.id,
    o.event_id,
    o.bracket,
    o.status,
    o.signal_at,
    o.model_probability,
    o.signal_ask,
    o.pnl
FROM paper_orders o
WHERE o.signal_at >= ?
  AND o.status IN ('SETTLED_WIN','SETTLED_LOSS')
ORDER BY o.id
""", (CUTOFF,)).fetchall()

for r in rows:
    m = re.search(r'between\s+(\d+)-(\d+)°F', r["bracket"])
    if not m:
        continue

    low = float(m.group(1))
    high = float(m.group(2))

    diag = db.execute("""
        SELECT diagnostics_json, model_version, as_of
        FROM live_model_diagnostics
        WHERE event_id=?
          AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    print("\n==============================")
    print("ORDER:", r["id"], "|", r["status"], "| PNL:", r["pnl"])
    print(r["bracket"])
    print("P(NO):", round(r["model_probability"], 4), "| ASK:", r["signal_ask"])
    print("BRACKET:", low, "-", high)

    if not diag:
        print("NO DIAGNOSTICS")
        continue

    d = json.loads(diag["diagnostics_json"])

    obs = d.get("in_day_observation")
    if obs:
        print("OBSERVED HIGH:", obs.get("high"))

    for c in d.get("components", []):
        mean = c.get("mean")
        if mean is None:
            continue

        if mean < low:
            position = "BELOW"
            distance = low - mean
        elif mean > high:
            position = "ABOVE"
            distance = mean - high
        else:
            position = "INSIDE"
            distance = 0

        print(
            c.get("model"),
            "| mean =", round(mean, 3),
            "| position =", position,
            "| distance =", round(distance, 3), "F"
        )

db.close()