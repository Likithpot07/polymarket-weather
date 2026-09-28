"""Step 2: read-only, as-of weather bracket probabilities. Python 3.11+."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import json
from math import erf, erfc, sqrt
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

VERSION = "baseline-mixture-v3-hourly-inday-sigma"
DEFAULT_FALLBACK_SIGMA_C = 2.5
DEFAULT_SIGMA_FLOOR_C = 1.5
DDL = """
CREATE TABLE IF NOT EXISTS probability_runs (
    id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, as_of TEXT NOT NULL,
    generated_at TEXT NOT NULL, model_version TEXT NOT NULL,
    city TEXT NOT NULL, target_date TEXT NOT NULL, unit TEXT NOT NULL,
    station TEXT, status TEXT NOT NULL, diagnostics_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bracket_probabilities (
    run_id INTEGER NOT NULL REFERENCES probability_runs(id),
    market_id TEXT NOT NULL, bracket TEXT NOT NULL,
    p_yes REAL NOT NULL, p_no REAL NOT NULL,
    PRIMARY KEY (run_id, market_id)
);
CREATE INDEX IF NOT EXISTS probability_event_time
    ON probability_runs(event_id, as_of);
"""


def parse_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("--as-of must have an explicit UTC offset, e.g. 2026-09-24T12:00:00Z")
    return dt.astimezone(timezone.utc)


def bracket(question: str) -> tuple[int | None, int | None, str] | None:
    """Integer temperature bins. None means an unbounded tail."""
    q = question.replace("−", "-").replace("–", "-").replace("—", "-")
    unit = re.search(r"°\s*([CF])\b|degrees?\s+(Celsius|Fahrenheit)\b", q, re.I)
    if not unit:
        return None
    scale = (unit.group(1) or unit.group(2)[0]).upper()
    match = re.search(r"(?<!\d)(-?\d{1,3})\s*(?:-|to|through)\s*(-?\d{1,3})\s*°?\s*[CF]?\b", q, re.I)
    if match:
        lo, hi = int(match.group(1)), int(match.group(2))
        return (lo, hi, scale) if lo <= hi else None
    match = re.search(r"(?<!\d)(-?\d{1,3})\s*°?\s*[CF]?\s*(?:or\s+)?(?:below|lower|less|under)\b", q, re.I)
    if match:
        return None, int(match.group(1)), scale
    match = re.search(r"(?<!\d)(-?\d{1,3})\s*°?\s*[CF]?\s*(?:or\s+)?(?:above|higher|more|over)\b", q, re.I)
    if match:
        return int(match.group(1)), None, scale
    match = re.search(r"(?<!\d)(-?\d{1,3})\s*°\s*[CF]\b", q, re.I)
    return (int(match.group(1)), int(match.group(1)), scale) if match else None


def event_markets(db: sqlite3.Connection, eid: str):
    rows = db.execute("""SELECT m.id,m.question,m.city,m.target_date,r.station,r.unit,r.verified
        FROM markets m JOIN rule_reviews r ON r.market_id=m.id
        WHERE m.event_id=? ORDER BY m.id""", (eid,)).fetchall()
    if not rows:
        raise ValueError(f"Event {eid}: no markets")
    cities = {r[2] for r in rows}
    days = {r[3] for r in rows}
    if None in cities or None in days or len(cities) != 1 or len(days) != 1:
        raise ValueError(f"Event {eid}: city or target date missing/inconsistent")
    parsed = []
    for mid, question, *_ in rows:
        b = bracket(question or "")
        if b is None:
            raise ValueError(f"Event {eid}: cannot parse bracket for market {mid}: {question}")
        parsed.append((mid, question, *b))
    units = {r[4] for r in parsed}
    units.update(r[5] for r in rows if r[5] in ("C", "F"))
    if len(units) != 1:
        raise ValueError(f"Event {eid}: conflicting bracket/rule units")
    bins = sorted(parsed, key=lambda r: float("-inf") if r[2] is None else r[2])
    if len(bins) < 2 or bins[0][2] is not None or bins[-1][3] is not None:
        raise ValueError(f"Event {eid}: missing lower or upper tail")
    if any(left[3] is None or right[2] is None or left[3] + 1 != right[2]
           for left, right in zip(bins, bins[1:])):
        raise ValueError(f"Event {eid}: brackets overlap or have gaps")
    if len({r[0] for r in bins}) != len(bins):
        raise ValueError(f"Event {eid}: duplicate market")
    stations = {r[4] for r in rows if r[4]}
    if len(stations) > 1:
        raise ValueError(f"Event {eid}: inconsistent stations")
    return bins, rows[0][2], rows[0][3], units.pop(), next(iter(stations), None), all(r[6] == 1 for r in rows)


def to_unit(temp_c: float, unit: str) -> float:
    return temp_c if unit == "C" else temp_c * 9 / 5 + 32


def in_day_observed_high(db: sqlite3.Connection, eid: str, station: str | None,
                         target: str, unit: str, tz: ZoneInfo,
                         as_of: datetime) -> dict | None:
    """Latest known target-day METAR maximum, deduplicated across bracket markets."""
    if not station or as_of.astimezone(tz).date() != date.fromisoformat(target):
        return None
    readings: dict[str, tuple[datetime, float]] = {}
    for observed, temp, stored_unit, fetched, record_id in db.execute(
        """SELECT o.observed_at,o.temperature,o.unit,o.fetched_at,o.source_record_id
           FROM observations o JOIN markets m ON o.market_id=m.id
           WHERE m.event_id=? AND o.station=? AND o.fetched_at<=?""",
        (eid, station, as_of.isoformat()),
    ):
        try:
            moment = parse_time(observed)
            if moment > as_of or moment.astimezone(tz).date().isoformat() != target:
                continue
            value = float(temp)
            value_c = value if stored_unit == "C" else (value - 32) * 5 / 9
            converted = to_unit(value_c, unit)
            key = str(record_id or moment.isoformat())
            readings[key] = (moment, converted)
        except (TypeError, ValueError):
            continue
    if not readings:
        return None
    values = list(readings.values())
    latest = max(moment for moment, _ in values)
    return {
        "station": station,
        "high": max(value for _, value in values),
        "unit": unit,
        "latest_observed_at": latest.isoformat(),
        "reading_count": len(values),
        "source": "AviationWeather METAR proxy",
    }


def latest_forecasts(db: sqlite3.Connection, city: str, target: str, as_of: datetime):
    """Use fetched_at, never a later model value for an earlier decision."""
    rows = db.execute("""SELECT b.model,b.fetched_at,v.temperature_c,b.id
        FROM forecast_batches b JOIN forecast_values v ON b.id=v.batch_id
        WHERE b.city=? AND v.period='daily_max' AND v.valid_time=? AND b.fetched_at<=?
        ORDER BY b.fetched_at DESC,b.id DESC""", (city, target, as_of.isoformat())).fetchall()
    result = {}
    for model, fetched, temp, batch in rows:
        if model not in result:
            result[model] = {"temperature_c": float(temp), "fetched_at": fetched, "batch_id": batch}
    return result


def in_day_hourly_context(
    db: sqlite3.Connection,
    components: list[dict],
    target: str,
    unit: str,
    tz: ZoneInfo,
    as_of: datetime,
) -> dict[str, dict]:
    """Remaining target-day hourly maxima from the same forecast batches used by the model."""
    target_day = date.fromisoformat(target)
    result: dict[str, dict] = {}
    for component in components:
        target_values: list[tuple[datetime, float]] = []
        for valid, temp_c in db.execute(
            """SELECT valid_time,temperature_c
               FROM forecast_values
               WHERE batch_id=? AND period='hourly'""",
            (component["batch_id"],),
        ):
            try:
                moment = parse_time(str(valid))
                if moment.astimezone(tz).date() != target_day:
                    continue
                target_values.append((moment, to_unit(float(temp_c), unit)))
            except (TypeError, ValueError):
                continue
        remaining = [value for moment, value in target_values if moment > as_of]
        result[str(component["model"])] = {
            "target_hourly_count": len(target_values),
            "remaining_hourly_count": len(remaining),
            "remaining_hourly_max": max(remaining) if remaining else None,
        }
    return result


def actual_highs(db: sqlite3.Connection, city: str, station: str,
                 tz: ZoneInfo, as_of: datetime) -> dict[str, float]:
    """METAR daily maxima are a calibration proxy, never a settlement result."""
    per_day: dict[str, dict[str, float]] = defaultdict(dict)
    today = as_of.astimezone(tz).date()

    # Generic historical station observations populated by calibrate.py.
    try:
        historical = db.execute(
            """SELECT observed_at,temperature_c,fetched_at
               FROM calibration_observations
               WHERE city=? AND station=? AND fetched_at<=?""",
            (city, station, as_of.isoformat()),
        ).fetchall()
    except sqlite3.OperationalError:
        historical = []
    for observed, temp_c, fetched in historical:
        try:
            moment = parse_time(observed)
            if moment > as_of or moment.astimezone(tz).date() >= today:
                continue
            day = moment.astimezone(tz).date().isoformat()
            per_day[day][moment.isoformat()] = float(temp_c)
        except (TypeError, ValueError):
            continue

    # Event-linked observations collected by collector.py. These are merged and
    # deduplicated so live data can coexist with calibration history.
    for observed, temp, unit, fetched in db.execute("""SELECT o.observed_at,o.temperature,o.unit,o.fetched_at
        FROM observations o JOIN markets m ON o.market_id=m.id
        WHERE m.city=? AND o.station=? AND o.fetched_at<=?""",
        (city, station, as_of.isoformat())):
        try:
            moment = parse_time(observed)
            if moment > as_of or moment.astimezone(tz).date() >= today:
                continue
            value = float(temp) if unit == "C" else (float(temp) - 32) * 5 / 9
            day = moment.astimezone(tz).date().isoformat()
            per_day[day][moment.isoformat()] = value
        except (TypeError, ValueError):
            continue
    # Incomplete days can badly skew max-temperature calibration.
    return {day: max(readings.values()) for day, readings in per_day.items()
            if len(readings) >= 18}


def calibration(db: sqlite3.Connection, city: str, station: str | None,
                unit: str, target: str, as_of: datetime, tz: ZoneInfo,
                min_days: int, sigma_floor: float) -> dict[str, dict]:
    if not station:
        return {}
    # Match earlier forecasts to observed, complete days with the same lead in calendar days.
    lead = max(0, (date.fromisoformat(target) - as_of.astimezone(tz).date()).days)
    highs = actual_highs(db, city, station, tz, as_of)
    residuals: dict[str, list[float]] = defaultdict(list)
    for day, actual_c in highs.items():
        if date.fromisoformat(day) >= date.fromisoformat(target):
            continue
        cutoff_local = datetime.combine(date.fromisoformat(day) - timedelta(days=lead),
                                        as_of.astimezone(tz).timetz().replace(tzinfo=None), tz)
        cutoff = min(cutoff_local.astimezone(timezone.utc), as_of)
        old = latest_forecasts(db, city, day, cutoff)
        for model, item in old.items():
            residuals[model].append(to_unit(actual_c, unit) - to_unit(item["temperature_c"], unit))
    result = {}
    for model, values in residuals.items():
        if len(values) < min_days:
            continue
        # Sample estimate, with a floor to prevent false certainty.
        bias = sum(values) / len(values)
        sigma = max(sigma_floor, sqrt(sum((x - bias) ** 2 for x in values) / (len(values) - 1)))
        result[model] = {"bias": bias, "sigma": sigma, "days": len(values),
                         "station": station, "source": "METAR proxy", "lead_days": lead}
    return result


def cdf(x: float, mean: float, sigma: float) -> float:
    return (1 + erf((x - mean) / (sigma * sqrt(2)))) / 2


def survival(x: float, mean: float, sigma: float) -> float:
    return 0.5 * erfc((x - mean) / (sigma * sqrt(2)))


def interval_probability(lower: float | None, upper: float | None,
                         mean: float, sigma: float) -> float:
    if lower is None:
        return cdf(upper, mean, sigma) if upper is not None else 1.0
    if upper is None:
        return survival(lower, mean, sigma)
    if lower >= upper:
        return 0.0
    if lower > mean:
        return max(0.0, survival(lower, mean, sigma) - survival(upper, mean, sigma))
    return max(0.0, cdf(upper, mean, sigma) - cdf(lower, mean, sigma))


def probability(lo: int | None, hi: int | None, mean: float, sigma: float) -> float:
    # Assumes settlement values are rounded to nearest whole unit.
    lower = None if lo is None else lo - 0.5
    upper = None if hi is None else hi + 0.5
    return interval_probability(lower, upper, mean, sigma)


def observed_floor_probability(lo: int | None, hi: int | None, components: list[dict],
                               observed_high: float) -> float:
    """Condition the mixture on the final daily maximum being at least the observed high."""
    numerator = 0.0
    denominator = 0.0
    lower = None if lo is None else lo - 0.5
    upper = None if hi is None else hi + 0.5
    for component in components:
        mean = float(component["mean"])
        sigma = float(component["sigma"])
        survival_mass = survival(observed_high, mean, sigma)
        denominator += survival_mass
        effective_lower = observed_high if lower is None else max(lower, observed_high)
        numerator += interval_probability(effective_lower, upper, mean, sigma)
    if denominator <= 0:
        raise ValueError("Observed high is outside the numerical support of every weather model")
    return max(0.0, min(1.0, numerator / denominator))


def evaluate(
    db: sqlite3.Connection,
    eid: str,
    as_of: datetime,
    min_days: int = 14,
    fallback_sigma_c: float = DEFAULT_FALLBACK_SIGMA_C,
    sigma_floor_c: float = DEFAULT_SIGMA_FLOOR_C,
    persist: bool = True,
) -> dict:
    if fallback_sigma_c <= 0 or sigma_floor_c <= 0:
        raise ValueError("sigma settings must be positive")
    if sigma_floor_c > fallback_sigma_c:
        raise ValueError("sigma floor cannot exceed fallback sigma")
    bins, city, target, unit, station, verified = event_markets(db, eid)
    loc = db.execute("SELECT timezone FROM locations WHERE city=?", (city,)).fetchone()
    if not loc:
        raise ValueError(f"Event {eid}: no location/timezone for {city}")
    tz = ZoneInfo(loc[0])
    if as_of.astimezone(tz).date() > date.fromisoformat(target):
        raise ValueError(f"Event {eid}: decision time is after target date")
    forecasts = latest_forecasts(db, city, target, as_of)
    if not forecasts:
        raise ValueError(f"Event {eid}: no forecast fetched by {as_of.isoformat()}")
    fallback_sigma = fallback_sigma_c if unit == "C" else fallback_sigma_c * 9 / 5
    sigma_floor = sigma_floor_c if unit == "C" else sigma_floor_c * 9 / 5
    trained = calibration(db, city, station, unit, target, as_of, tz, min_days, sigma_floor)
    components = []
    for model, item in sorted(forecasts.items()):
        fit = trained.get(model)
        bias = fit["bias"] if fit else 0.0
        daily_mean = to_unit(item["temperature_c"], unit) + bias
        components.append({
            "model": model,
            "mean": daily_mean,
            "daily_mean": daily_mean,
            "bias": bias,
            "sigma": fit["sigma"] if fit else fallback_sigma,
            "calibration_days": fit["days"] if fit else 0,
            "fetched_at": item["fetched_at"],
            "batch_id": item["batch_id"],
        })
    observed = in_day_observed_high(db, eid, station, target, unit, tz, as_of)
    hourly_context: dict[str, dict] = {}
    if observed:
        hourly_context = in_day_hourly_context(db, components, target, unit, tz, as_of)
        for component in components:
            context = hourly_context.get(str(component["model"]), {})
            remaining_max = context.get("remaining_hourly_max")
            target_count = int(context.get("target_hourly_count") or 0)
            if remaining_max is not None:
                biased_remaining_max = float(remaining_max) + float(component["bias"])
                component["remaining_hourly_max"] = biased_remaining_max
                component["mean"] = max(float(observed["high"]), biased_remaining_max)
                component["in_day_mean_source"] = "observed_high_plus_remaining_hourly"
            elif target_count > 0:
                component["remaining_hourly_max"] = None
                component["mean"] = float(observed["high"])
                component["in_day_mean_source"] = "observed_high_no_remaining_hours"
            else:
                component["remaining_hourly_max"] = None
                component["in_day_mean_source"] = "daily_max_fallback_no_hourly"
    probs = []
    for mid, label, lo, hi, _ in bins:
        if observed:
            p = observed_floor_probability(lo, hi, components, observed["high"])
        else:
            p = sum(probability(lo, hi, c["mean"], c["sigma"]) for c in components) / len(components)
        probs.append({"market_id": mid, "bracket": label, "p_yes": p, "p_no": 1 - p})
    total = sum(p["p_yes"] for p in probs)
    if abs(total - 1) > 1e-8:
        raise ValueError(f"Event {eid}: probabilities do not cover the event: {total}")
    reasons = []
    if not verified:
        reasons.append("resolution rules not manually verified")
    if len(trained) != len(components):
        reasons.append("some models lack enough historical station days; fallback spread used")
    if observed:
        if components and all(c.get("in_day_mean_source") != "daily_max_fallback_no_hourly" for c in components):
            reasons.append("target-day METAR high combined with remaining hourly forecasts")
        else:
            reasons.append("target-day METAR high modeled; some models lack remaining hourly forecasts")
    reasons.append("METAR maximum may differ from official finalized resolution temperature")
    reasons.append("forecast coordinates may differ from resolution station")
    reasons.append("nearest-whole-degree rounding assumed")
    result = {
        "event_id": eid,
        "city": city,
        "target_date": target,
        "unit": unit,
        "station": station,
        "as_of": as_of.isoformat(),
        "status": "provisional",
        "notes": reasons,
        "components": components,
        "in_day_observation": observed,
        "in_day_hourly_context": hourly_context,
        "in_day_observations_modeled": bool(observed),
        "probabilities": probs,
    }
    if persist:
        with db:
            cursor = db.execute(
                """INSERT INTO probability_runs
                (event_id,as_of,generated_at,model_version,city,target_date,unit,station,status,diagnostics_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    eid,
                    as_of.isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    VERSION,
                    city,
                    target,
                    unit,
                    station,
                    result["status"],
                    json.dumps({
                        "notes": reasons,
                        "components": components,
                        "min_calibration_days": min_days,
                        "fallback_sigma_c": fallback_sigma_c,
                        "sigma_floor_c": sigma_floor_c,
                        "in_day_observation": observed,
                        "in_day_hourly_context": hourly_context,
                        "in_day_observations_modeled": bool(observed),
                    }),
                ),
            )
            result["run_id"] = cursor.lastrowid
            db.executemany(
                "INSERT INTO bracket_probabilities VALUES (?,?,?,?,?)",
                [
                    (cursor.lastrowid, p["market_id"], p["bracket"], p["p_yes"], p["p_no"])
                    for p in probs
                ],
            )
    return result

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--event-id", required=True, help="Gamma event ID, not an individual bracket market ID")
    ap.add_argument("--as-of", default=datetime.now(timezone.utc).isoformat())
    ap.add_argument("--min-calibration-days", type=int, default=14)
    ap.add_argument("--fallback-sigma-c", type=float, default=DEFAULT_FALLBACK_SIGMA_C)
    ap.add_argument("--sigma-floor-c", type=float, default=DEFAULT_SIGMA_FLOOR_C)
    args = ap.parse_args()
    if (args.min_calibration_days < 2 or args.fallback_sigma_c <= 0
            or args.sigma_floor_c <= 0 or args.sigma_floor_c > args.fallback_sigma_c):
        ap.error("min-calibration-days must be >= 2 and 0 < sigma-floor-c <= fallback-sigma-c")
    config_file = Path(args.config).resolve()
    cfg = json.loads(config_file.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_file.parent / db_path
    if not db_path.is_file():
        ap.error(f"Collector database does not exist: {db_path}")
    db = sqlite3.connect(db_path)
    try:
        db.executescript(DDL)
        result = evaluate(
            db,
            args.event_id,
            parse_time(args.as_of),
            args.min_calibration_days,
            args.fallback_sigma_c,
            args.sigma_floor_c,
        )
        print(json.dumps(result, indent=2))
    except (ValueError, sqlite3.Error) as exc:
        ap.error(str(exc))
    finally:
        db.close()


if __name__ == "__main__":
    main()
