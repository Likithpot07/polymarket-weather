import sqlite3
import json
from probability import bracket as parse_bracket
from probability import probability as bracket_probability
from probability import observed_floor_probability

db = sqlite3.connect("data/weather.sqlite3")
db.row_factory = sqlite3.Row

rows = db.execute("""
SELECT
    o.*,
    m.target_date,
    CASE WHEN substr(o.signal_at,1,10)=m.target_date THEN 1 ELSE 0 END AS was_target_day
FROM paper_orders o
JOIN markets m ON m.id=o.market_id
WHERE o.status IN ('SETTLED_WIN','SETTLED_LOSS')
ORDER BY o.settled_at
""").fetchall()

print("\n=== TARGET DAY VS PRE-TARGET ===")

groups = {}

for r in rows:
    key = r["was_target_day"]

    if key not in groups:
        groups[key] = {
            "n": 0,
            "wins": 0,
            "losses": 0,
            "pnl": 0.0
        }

    groups[key]["n"] += 1
    groups[key]["wins"] += r["status"] == "SETTLED_WIN"
    groups[key]["losses"] += r["status"] == "SETTLED_LOSS"
    groups[key]["pnl"] += r["pnl"] or 0

for key, x in groups.items():
    win_rate = x["wins"] / x["n"] * 100 if x["n"] else 0

    print(
        "target_day =", bool(key),
        "| n =", x["n"],
        "| wins =", x["wins"],
        "| losses =", x["losses"],
        "| win_rate =", round(win_rate, 2),
        "| pnl =", round(x["pnl"], 2)
    )

print("\n=== WINNING NO TRADES ===")

winners = [
    r for r in rows
    if r["side"] == "NO"
    and r["status"] == "SETTLED_WIN"
]

for r in winners:
    gap = r["model_probability"] - r["signal_ask"]

    print(
        "\nORDER:", r["id"],
        "| EVENT:", r["event_id"],
        "| ASK:", r["signal_ask"],
        "| P(NO):", round(r["model_probability"], 4),
        "| GAP:", round(gap, 4),
        "| TARGET_DAY:", bool(r["was_target_day"]),
        "| COST:", round(r["total_cost"], 2),
        "| PAYOUT:", round(r["payout"], 2),
        "| PNL:", round(r["pnl"], 2)
    )

    print(r["bracket"])

    diag = db.execute("""
        SELECT
            d.model_version,
            d.diagnostics_json,
            d.as_of
        FROM live_model_diagnostics d
        WHERE d.event_id=?
          AND d.as_of<=?
        ORDER BY d.as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        print("NO DIAGNOSTICS FOUND")
        continue

    d = json.loads(diag["diagnostics_json"])

    print("MODEL:", diag["model_version"])
    print("MODEL RUN:", diag["as_of"])
    print("OBSERVED:", d.get("in_day_observation"))

    for c in d.get("components", []):
        print(
            c.get("model"),
            "| mean =", c.get("mean"),
            "| sigma =", c.get("sigma"),
            "| source =", c.get("in_day_mean_source"),
            "| remaining max =", c.get("remaining_hourly_max")
        )

print("\n=== LOSING NO TRADES ===")

losers = [
    r for r in rows
    if r["side"] == "NO"
    and r["status"] == "SETTLED_LOSS"
]

for r in losers:
    gap = r["model_probability"] - r["signal_ask"]

    print(
        "\nORDER:", r["id"],
        "| EVENT:", r["event_id"],
        "| ASK:", r["signal_ask"],
        "| P(NO):", round(r["model_probability"], 4),
        "| GAP:", round(gap, 4),
        "| TARGET_DAY:", bool(r["was_target_day"]),
        "| COST:", round(r["total_cost"], 2),
        "| PAYOUT:", round(r["payout"], 2),
        "| PNL:", round(r["pnl"], 2)
    )

    print(r["bracket"])

    diag = db.execute("""
        SELECT
            d.model_version,
            d.diagnostics_json,
            d.as_of
        FROM live_model_diagnostics d
        WHERE d.event_id=?
          AND d.as_of<=?
        ORDER BY d.as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        print("NO DIAGNOSTICS FOUND")
        continue

    d = json.loads(diag["diagnostics_json"])

    print("MODEL:", diag["model_version"])
    print("MODEL RUN:", diag["as_of"])
    print("OBSERVED:", d.get("in_day_observation"))

    for c in d.get("components", []):
        print(
            c.get("model"),
            "| mean =", c.get("mean"),
            "| sigma =", c.get("sigma"),
            "| source =", c.get("in_day_mean_source"),
            "| remaining max =", c.get("remaining_hourly_max")
        )

    print("\n=== ACTUAL HIGH VS MODEL FORECAST ===")

for r in rows:
    if r["event_id"] not in ("1090393", "1090135", "1090136"):
        continue

    diag = db.execute("""
        SELECT diagnostics_json, model_version, as_of
        FROM live_model_diagnostics
        WHERE event_id=?
          AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        continue

    d = json.loads(diag["diagnostics_json"])

    market = db.execute("""
        SELECT city, target_date
        FROM markets
        WHERE id=?
    """, (r["market_id"],)).fetchone()

    city = market["city"]
    target_date = market["target_date"]

    obs = db.execute("""
        SELECT
            MAX(
                CASE
                    WHEN o.unit='F' THEN o.temperature
                    ELSE o.temperature * 9.0 / 5.0 + 32
                END
            ) AS actual_high
        FROM observations o
        JOIN markets m ON m.id=o.market_id
        WHERE m.event_id=?
    """, (r["event_id"],)).fetchone()

    actual_high = obs["actual_high"]

    print("\n==============================")
    print("ORDER:", r["id"], "|", r["status"])
    print("CITY:", city, "| DATE:", target_date)
    print(r["bracket"])
    print("OBSERVED FINAL HIGH:", actual_high)

    for c in d.get("components", []):
        mean = c.get("mean")
        if mean is None or actual_high is None:
            continue

        print(
            c.get("model"),
            "| forecast =", round(mean, 2),
            "| actual =", round(actual_high, 2),
            "| error =", round(mean - actual_high, 2), "F"
        )

    print("\n=== SIGMA INVESTIGATION ===")

SIGMAS = [1.5, 2.0, 2.7, 3.5, 4.5]

for r in rows:
    if r["signal_at"] < "2026-09-29T08:13:04+00:00":
        continue

    diag = db.execute("""
        SELECT diagnostics_json, model_version, as_of
        FROM live_model_diagnostics
        WHERE event_id=?
          AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        continue

    parsed = parse_bracket(r["bracket"])
    if not parsed:
        continue

    lo, hi, unit = parsed
    d = json.loads(diag["diagnostics_json"])
    components = d.get("components", [])

    print("\n==============================")
    print("ORDER:", r["id"], "|", r["status"], "| PNL:", round(r["pnl"], 2))
    print(r["bracket"])
    print("ACTUAL TRADE P(NO):", round(r["model_probability"], 4))

    print("CURRENT COMPONENTS:")
    for c in components:
        print(
            c.get("model"),
            "| mean =", round(c["mean"], 3),
            "| sigma =", round(c["sigma"], 3)
        )

    print("HYPOTHETICAL SIGMA RESULTS:")

    for sigma in SIGMAS:
        p_yes = sum(
            bracket_probability(
                lo,
                hi,
                float(c["mean"]),
                sigma
            )
            for c in components
        ) / len(components)

        print(
            "sigma =", sigma,
            "| P(YES) =", round(p_yes, 4),
            "| P(NO) =", round(1 - p_yes, 4)
        )

    print("\n=== EXACT PRODUCTION SIGMA REPLAY ===")

SIGMAS = [1.5, 2.0, 2.7, 3.5, 4.5]

for r in rows:
    if r["signal_at"] < "2026-09-29T08:13:04+00:00":
        continue

    diag = db.execute("""
        SELECT diagnostics_json, model_version, as_of
        FROM live_model_diagnostics
        WHERE event_id=?
          AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        continue

    parsed = parse_bracket(r["bracket"])
    if not parsed:
        continue

    lo, hi, unit = parsed
    d = json.loads(diag["diagnostics_json"])

    observed = d.get("in_day_observation")
    components = d.get("components", [])

    if not observed or not components:
        continue

    print("\n==============================")
    print("ORDER:", r["id"], "|", r["status"], "| PNL:", round(r["pnl"], 2))
    print(r["bracket"])
    print("ACTUAL TRADE P(NO):", round(r["model_probability"], 4))
    print("OBSERVED HIGH:", observed["high"])

    for sigma in SIGMAS:
        test_components = []

        for c in components:
            x = dict(c)
            x["sigma"] = sigma
            test_components.append(x)

        p_yes = observed_floor_probability(
            lo,
            hi,
            test_components,
            float(observed["high"])
        )

        p_no = 1 - p_yes

        print(
            "sigma =", sigma,
            "| P(YES) =", round(p_yes, 4),
            "| P(NO) =", round(p_no, 4),
            "| would_trade =", p_no >= 0.70
        )

    print("\n=== V3 TARGET-DAY SIGMA CAP 2.0F BACKTEST ===")

kept = []
blocked = []

for r in rows:
    if not r["was_target_day"]:
        continue

    diag = db.execute("""
        SELECT diagnostics_json, model_version, as_of
        FROM live_model_diagnostics
        WHERE event_id=?
          AND as_of<=?
        ORDER BY as_of DESC
        LIMIT 1
    """, (r["event_id"], r["signal_at"])).fetchone()

    if not diag:
        continue

    if diag["model_version"] != "baseline-mixture-v3-hourly-inday-sigma":
        continue

    parsed = parse_bracket(r["bracket"])
    if not parsed:
        continue

    lo, hi, unit = parsed
    d = json.loads(diag["diagnostics_json"])

    observed = d.get("in_day_observation")
    components = d.get("components", [])

    if not observed or not components:
        continue

    sigma_cap = 2.0 if unit == "F" else 2.0 * 5.0 / 9.0

    capped_components = []

    for c in components:
        x = dict(c)
        x["sigma"] = min(float(c["sigma"]), sigma_cap)
        capped_components.append(x)

    p_yes = observed_floor_probability(
        lo,
        hi,
        capped_components,
        float(observed["high"])
    )

    p_no = 1 - p_yes
    gap = p_no - float(r["signal_ask"])

    would_trade = (
        p_no >= 0.70
        and gap <= 0.40
    )

    result = {
        "id": r["id"],
        "status": r["status"],
        "old_p": r["model_probability"],
        "new_p": p_no,
        "ask": r["signal_ask"],
        "gap": gap,
        "pnl": r["pnl"] or 0,
        "bracket": r["bracket"]
    }

    if would_trade:
        kept.append(result)
    else:
        blocked.append(result)

    print(
        "\nORDER:", r["id"],
        "|", r["status"],
        "| old P(NO):", round(r["model_probability"], 4),
        "| capped P(NO):", round(p_no, 4),
        "| gap:", round(gap, 4),
        "| TRADE:", would_trade,
        "| PNL:", round(r["pnl"] or 0, 2)
    )
    print(r["bracket"])

print("\n=== SUMMARY ===")

print("original trades =", len(kept) + len(blocked))
print("kept =", len(kept))
print("blocked =", len(blocked))

print(
    "kept wins =",
    sum(x["status"] == "SETTLED_WIN" for x in kept)
)

print(
    "kept losses =",
    sum(x["status"] == "SETTLED_LOSS" for x in kept)
)

print(
    "simulated pnl =",
    round(sum(x["pnl"] for x in kept), 2)
)

print(
    "blocked wins =",
    sum(x["status"] == "SETTLED_WIN" for x in blocked)
)

print(
    "blocked losses =",
    sum(x["status"] == "SETTLED_LOSS" for x in blocked)
)

db.close()