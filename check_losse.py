"""What do the new bot's losing bets have in common? Compares losses with wins.

    python check_losse.py

Read-only. Old-bot bets (legacy=1) are ignored. "Actual high" is the airport METAR
maximum we collected, a proxy for the official settlement number, not the same thing.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from probability import bracket as parse_bracket, parse_time


def forecast_at_entry(db, event_id: str, signal_at: str) -> dict:
    """Combined forecast mean/sigma and model disagreement the bot saw when it bet."""
    row = db.execute(
        """SELECT diagnostics_json FROM live_model_diagnostics
           WHERE event_id=? AND as_of<=? ORDER BY as_of DESC LIMIT 1""",
        (event_id, signal_at)).fetchone()
    if not row:
        return {}
    d = json.loads(row[0])
    comps = [c for c in d.get("components", []) if c.get("mean") is not None]
    means = [float(c["mean"]) for c in comps]
    combined = d.get("combined") or {}
    mean = combined.get("mean")
    sigma = combined.get("sigma")
    if mean is None and means:
        mean = sum(means) / len(means)
        sigma = sum(float(c.get("sigma") or 0) for c in comps) / len(comps)
    return {
        "mean": None if mean is None else float(mean),
        "sigma": None if sigma is None else float(sigma),
        "spread": (max(means) - min(means)) if len(means) > 1 else None,
        "models": {str(c.get("model")): round(float(c["mean"]), 1) for c in comps},
    }


def nws_at_entry(db, city: str, target: str, signal_at: str, unit: str) -> float | None:
    """US National Weather Service forecast high for the target day, as known when we bet."""
    row = db.execute(
        """SELECT v.temperature_c FROM forecast_batches b JOIN forecast_values v ON v.batch_id=b.id
           WHERE b.city=? AND b.model='nws' AND v.period='nws_daily_max' AND v.valid_time=?
             AND b.fetched_at<=? ORDER BY b.fetched_at DESC,b.id DESC LIMIT 1""",
        (city, target, signal_at)).fetchone()
    if not row:
        return None
    return float(row[0]) if unit == "C" else float(row[0]) * 9 / 5 + 32


def actual_high(db, event_id: str, target: str, tz_name: str | None, unit: str) -> float | None:
    tz = ZoneInfo(tz_name or "UTC")
    best = None
    for observed_at, temp, obs_unit in db.execute(
            """SELECT o.observed_at,o.temperature,o.unit FROM observations o
               JOIN markets m ON m.id=o.market_id WHERE m.event_id=?""", (event_id,)):
        try:
            if parse_time(str(observed_at)).astimezone(tz).date().isoformat() != target:
                continue
        except ValueError:
            continue
        t = float(temp)
        if obs_unit != unit:
            t = t * 9 / 5 + 32 if unit == "F" else (t - 32) * 5 / 9
        best = t if best is None else max(best, t)
    return best


def distance_outside(lo, hi, value: float) -> float:
    """How far the value sits outside the bracket (negative = inside it)."""
    low = -1e9 if lo is None else lo - 0.5
    high = 1e9 if hi is None else hi + 0.5
    if value < low:
        return low - value
    if value > high:
        return value - high
    return -min(value - low, high - value)


def band(x, edges, labels):
    if x is None:
        return "unknown"
    for edge, label in zip(edges, labels):
        if x < edge:
            return label
    return labels[-1]


def main() -> None:
    cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
    db = sqlite3.connect(f"file:{cfg['db_path']}?mode=ro", uri=True, timeout=60)
    rows = db.execute(
        """SELECT p.id,p.event_id,p.market_id,p.bracket,p.signal_at,p.vwap,p.model_probability,
                  p.pnl,p.status,m.city,m.target_date,l.timezone
           FROM paper_orders p JOIN markets m ON m.id=p.market_id
           LEFT JOIN locations l ON l.city=m.city
           WHERE p.legacy=0 AND p.status IN ('SETTLED_WIN','SETTLED_LOSS')
           ORDER BY p.signal_at""").fetchall()

    bets = []
    for oid, eid, mid, bracket, signal_at, vwap, p_no, pnl, status, city, target, tz_name in rows:
        parsed = parse_bracket(bracket or "")
        lo, hi, unit = parsed if parsed else (None, None, "F")
        try:
            lead = (date.fromisoformat(target)
                    - parse_time(signal_at).astimezone(ZoneInfo(tz_name or "UTC")).date()).days
        except (TypeError, ValueError):
            lead = None
        edge = db.execute(
            """SELECT net_edge FROM paper_candidate_audit
               WHERE event_id=? AND market_id=? AND selected=1 AND as_of<=?
               ORDER BY as_of DESC LIMIT 1""", (eid, mid, signal_at)).fetchone()
        fc = forecast_at_entry(db, eid, signal_at)
        actual = actual_high(db, eid, target, tz_name, unit)
        nws = nws_at_entry(db, city, target, signal_at, unit)
        dist = distance_outside(lo, hi, fc["mean"]) if fc.get("mean") is not None and parsed else None
        bets.append({
            "id": oid, "won": status == "SETTLED_WIN", "pnl": float(pnl or 0), "city": city,
            "target": target, "lead": lead, "bracket": bracket, "unit": unit,
            "tail": parsed is not None and (lo is None or hi is None),
            "paid": float(vwap or 0), "p_no": float(p_no or 0),
            "edge": None if not edge or edge[0] is None else float(edge[0]),
            "mean": fc.get("mean"), "sigma": fc.get("sigma"), "spread": fc.get("spread"),
            "models": fc.get("models"), "actual": actual, "dist": dist,
            "dist_sigma": dist / fc["sigma"] if dist is not None and fc.get("sigma") else None,
            "miss": None if actual is None or fc.get("mean") is None else actual - fc["mean"],
            "nws": nws, "nws_dist": distance_outside(lo, hi, nws) if nws is not None and parsed else None,
            "nws_miss": None if actual is None or nws is None else actual - nws,
        })

    f = lambda x, n=1: "NA" if x is None else f"{x:.{n}f}"

    print("=== 1. EACH LOSING BET ===")
    for b in [b for b in bets if not b["won"]]:
        print(f"\n#{b['id']} {b['city']} {b['target']} | {b['lead']} days ahead | pnl {b['pnl']:+.2f}")
        print(f"  bracket: {b['bracket']}")
        print(f"  paid {f(b['paid'], 2)} | model P(NO) {f(b['p_no'], 2)} | edge {f(None if b['edge'] is None else b['edge'] * 100)}%")
        print(f"  forecast {f(b['mean'])}{b['unit']} (sigma {f(b['sigma'])}, models disagree by {f(b['spread'])})"
              f" -> {f(b['dist'])}{b['unit']} = {f(b['dist_sigma'])} sigma outside bracket")
        print(f"  per model: {b['models']}")
        if b["nws"] is None:
            print("  NWS forecast: none (non-US city, or bet placed before NWS collection)")
        else:
            print(f"  NWS forecast {f(b['nws'])}{b['unit']} -> {f(b['nws_dist'])}{b['unit']} outside bracket"
                  + (" (NWS said it lands IN the bracket)" if b["nws_dist"] <= 0 else ""))
        nws_miss = "" if b["nws_miss"] is None else f" | NWS miss {f(b['nws_miss'])}{b['unit']}"
        print(f"  actual high (METAR) {f(b['actual'])}{b['unit']} | our miss {f(b['miss'])}{b['unit']}{nws_miss}")

    print("\n=== 2. LOSSES vs WINS (averages) ===")
    for label, key in [("price paid", "paid"), ("model P(NO)", "p_no"), ("edge", "edge"),
                       ("days ahead", "lead"), ("model disagreement", "spread"),
                       ("forecast sigma", "sigma"), ("distance outside bracket (sigma)", "dist_sigma")]:
        out = []
        for name, group in (("losses", [b for b in bets if not b["won"]]), ("wins", [b for b in bets if b["won"]])):
            vals = [b[key] for b in group if b[key] is not None]
            out.append(f"{name} {f(sum(vals) / len(vals), 2) if vals else 'NA'}")
        print(f"  {label:<34} " + " | ".join(out))
    misses = [abs(b["miss"]) for b in bets if b["miss"] is not None]
    lmiss = [abs(b["miss"]) for b in bets if b["miss"] is not None and not b["won"]]
    print(f"  {'avg forecast miss (abs)':<34} losses {f(sum(lmiss) / len(lmiss), 2) if lmiss else 'NA'}"
          f" | all {f(sum(misses) / len(misses), 2) if misses else 'NA'}")

    both = [b for b in bets if b["miss"] is not None and b["nws_miss"] is not None]
    print(f"\n=== 2b. OUR FORECAST vs NWS (US cities, bets where both exist: {len(both)}) ===")
    if both:
        ours = sum(abs(b["miss"]) for b in both) / len(both)
        theirs = sum(abs(b["nws_miss"]) for b in both) / len(both)
        print(f"  avg miss: ours {ours:.2f} | NWS {theirs:.2f}  (lower is better)")
        warned = [b for b in both if b["nws_dist"] is not None and b["nws_dist"] <= 0.5]
        print(f"  bets where NWS put the high in/near our bracket: {len(warned)}"
              f" -> {sum(not b['won'] for b in warned)} of them lost")
    else:
        print("  no overlap yet (NWS collection started recently)")

    def table(title, keyfn):
        print(f"\n=== {title} ===")
        groups = defaultdict(list)
        for b in bets:
            groups[keyfn(b)].append(b)
        for key in sorted(groups, key=str):
            g = groups[key]
            wins = sum(b["won"] for b in g)
            print(f"  {str(key):<22} bets {len(g):>3} | wins {wins:>3} | losses {len(g) - wins:>2}"
                  f" | win% {100 * wins / len(g):5.1f} | profit {sum(b['pnl'] for b in g):+7.2f}")

    table("3. BY CITY", lambda b: b["city"])
    table("4. BY DAYS AHEAD", lambda b: b["lead"])
    table("5. BY BRACKET TYPE", lambda b: "tail (X or above/below)" if b["tail"] else "middle range")
    table("6. BY DISTANCE OUTSIDE BRACKET", lambda b: band(
        b["dist_sigma"], [0, 1, 2], ["a) forecast inside", "b) 0-1 sigma", "c) 1-2 sigma", "d) 2+ sigma"]))
    table("7. BY MODEL DISAGREEMENT", lambda b: band(
        b["spread"], [0.5, 1.0, 1.5], ["a) <0.5", "b) 0.5-1.0", "c) 1.0-1.5", "d) 1.5+"]))
    table("8. BY PRICE PAID", lambda b: band(
        b["paid"], [0.6, 0.7, 0.8], ["a) <0.60", "b) 0.60-0.70", "c) 0.70-0.80", "d) 0.80+"]))
    print("\nSmall sample: treat any pattern from 8 losses as a hint, not proof.")
    db.close()


if __name__ == "__main__":
    main()
