"""Step 3: learn how accurate each forecast model is at each settlement airport.

For every city the bot trades, this downloads:
  - past forecasts at the airport, as they looked 0, 1, 2 and 3 days ahead
    (Open-Meteo Previous Runs API), and
  - what the airport actually recorded (Iowa Environmental Mesonet METAR archive).
It stores one row per city/model/lead/day in station_residuals. probability.py
then uses the most recent 90 days *before* each decision (no peeking at the
future) to correct each model's bias and set a realistic spread.

    python station_history.py --days 150

Safe to re-run; it replaces rows. Stop collector.py/live.py first, then restart
them afterwards. Paste the printed summary back to Claude.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
import sqlite3
from statistics import mean, pstdev
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from http.client import HTTPException
from zoneinfo import ZoneInfo

import truststore

from collector import station_coordinates

# Use Windows' certificate store, same as calibrate.py (fixes SSL errors behind
# antivirus/ISP certificate inspection).
truststore.inject_into_ssl()

PREVIOUS_RUNS = "https://previous-runs-api.open-meteo.com/v1/forecast"
CHUNK_DAYS = 30  # smaller downloads are far less likely to be cut off
IEM_ASOS = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
LEADS = (0, 1, 2, 3)
MIN_READINGS_PER_DAY = 18
MIN_FORECAST_HOURS = 20

RESIDUAL_DDL = """
CREATE TABLE IF NOT EXISTS station_residuals (
    city TEXT NOT NULL, station TEXT NOT NULL, model TEXT NOT NULL,
    lead_days INTEGER NOT NULL, day TEXT NOT NULL,
    forecast_c REAL NOT NULL, actual_c REAL NOT NULL, residual_c REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (city, station, model, lead_days, day)
);
CREATE INDEX IF NOT EXISTS station_residual_lookup
    ON station_residuals(city, model, lead_days, day);
"""


def http_get(url: str, attempts: int = 4) -> bytes:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"User-Agent": "weather-calibration/0.1"})
            with urlopen(request, timeout=90) as response:
                return response.read()
        except HTTPError as exc:
            if 400 <= exc.code < 500 and exc.code != 429:
                raise
            last = exc
        except (URLError, TimeoutError, OSError, HTTPException) as exc:
            last = exc
        time.sleep(min(30, 3 * 2 ** attempt))
    reason = getattr(last, "reason", None) or last
    raise URLError(f"failed after {attempts} attempts ({type(last).__name__}: {reason})")


def chunks(start: date, end: date):
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=CHUNK_DAYS - 1))
        yield cursor, stop
        cursor = stop + timedelta(days=1)


def station_daily_highs(station: str, start: date, end: date, tz: ZoneInfo) -> dict[str, float]:
    """Local-day maximum (Celsius) of routine + special METARs, downloaded in chunks."""
    highs: dict[str, float] = {}
    for a, b in chunks(start, end):
        highs.update(_station_daily_highs(station, a, b, tz))
    return highs


def _station_daily_highs(station: str, start: date, end: date, tz: ZoneInfo) -> dict[str, float]:
    candidates = [station[1:]] if len(station) == 4 and station.startswith("K") else []
    candidates.append(station)
    for code in candidates:
        params = [("station", code), ("data", "tmpf"), ("tz", "Etc/UTC"), ("format", "onlycomma"),
                  ("latlon", "no"), ("missing", "M"), ("direct", "no"),
                  ("report_type", "3"), ("report_type", "4"),
                  ("year1", (start - timedelta(days=1)).year), ("month1", (start - timedelta(days=1)).month),
                  ("day1", (start - timedelta(days=1)).day),
                  ("year2", (end + timedelta(days=2)).year), ("month2", (end + timedelta(days=2)).month),
                  ("day2", (end + timedelta(days=2)).day)]
        try:
            text = http_get(f"{IEM_ASOS}?{urlencode(params)}").decode("utf-8", "replace")
        except (HTTPError, URLError) as exc:
            print(f"    IEM {code}: {exc}")
            continue
        readings: dict[str, dict[str, float]] = defaultdict(dict)
        for row in csv.DictReader(line for line in io.StringIO(text) if not line.startswith("#")):
            try:
                moment = datetime.strptime(row["valid"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                value_c = (float(row["tmpf"]) - 32) * 5 / 9
            except (KeyError, TypeError, ValueError):
                continue
            local_day = moment.astimezone(tz).date()
            if start <= local_day <= end:
                readings[local_day.isoformat()][moment.isoformat()] = value_c
        highs = {day: max(v.values()) for day, v in readings.items() if len(v) >= MIN_READINGS_PER_DAY}
        if highs:
            return highs
    return {}


def forecast_daily_highs(model: str, lat: float, lon: float, tz_name: str,
                         start: date, end: date) -> dict[int, dict[str, float]]:
    """{lead_days: {local_day: forecast daily max C}}, downloaded in chunks."""
    merged: dict[int, dict[str, float]] = defaultdict(dict)
    errors = []
    for a, b in chunks(start, end):
        try:
            for lead, per_day in _forecast_daily_highs(model, lat, lon, tz_name, a, b).items():
                merged[lead].update(per_day)
        except (HTTPError, URLError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{a}: {exc}")
        time.sleep(0.3)
    if not any(merged.values()):
        raise ValueError("; ".join(errors[:2]) or "no data returned")
    return dict(merged)


def _forecast_daily_highs(model: str, lat: float, lon: float, tz_name: str,
                          start: date, end: date) -> dict[int, dict[str, float]]:
    variables = ["temperature_2m"] + [f"temperature_2m_previous_day{lead}" for lead in LEADS if lead]
    params = {"latitude": lat, "longitude": lon, "hourly": ",".join(variables), "models": model,
              "timezone": tz_name, "temperature_unit": "celsius",
              "start_date": start.isoformat(), "end_date": end.isoformat()}
    payload = json.loads(http_get(f"{PREVIOUS_RUNS}?{urlencode(params)}"))
    if payload.get("error"):
        raise ValueError(payload.get("reason", "API error"))
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    result: dict[int, dict[str, float]] = {}
    for lead in LEADS:
        key = "temperature_2m" if lead == 0 else f"temperature_2m_previous_day{lead}"
        values = hourly.get(key)
        if not values:
            continue
        per_day: dict[str, list[float]] = defaultdict(list)
        for stamp, value in zip(times, values):
            if value is not None:
                per_day[str(stamp)[:10]].append(float(value))
        result[lead] = {day: max(v) for day, v in per_day.items() if len(v) >= MIN_FORECAST_HOURS}
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--days", type=int, default=150, help="History length (days)")
    ap.add_argument("--city", action="append", help="Limit to a city; repeatable")
    args = ap.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    db_path = db_path if db_path.is_absolute() else config_path.parent / db_path
    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(RESIDUAL_DDL)

    cities = db.execute(
        """SELECT m.city, r.station, l.latitude, l.longitude, l.timezone, COUNT(*)
           FROM markets m JOIN rule_reviews r ON r.market_id=m.id
           JOIN locations l ON l.city=m.city
           WHERE r.station IS NOT NULL AND m.target_date >= ?
           GROUP BY m.city, r.station ORDER BY COUNT(*) DESC""",
        ((date.today() - timedelta(days=30)).isoformat(),),
    ).fetchall()
    chosen: dict[str, tuple] = {}
    for city, station, lat, lon, tz_name, _ in cities:
        chosen.setdefault(city, (station, lat, lon, tz_name))  # most-used station per city
    if args.city:
        chosen = {c: v for c, v in chosen.items() if c in set(args.city)}

    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=args.days)
    models = cfg["weather_models"]
    summary = []
    print(f"Cities: {len(chosen)} | history {start} -> {end} | models {', '.join(models)}")
    # Quick connection check so a network problem is obvious straight away.
    for name, url in (("Open-Meteo history", f"{PREVIOUS_RUNS}?latitude=40.78&longitude=-73.88"
                       "&hourly=temperature_2m_previous_day1&past_days=2&forecast_days=1"),
                      ("Iowa Mesonet METAR", f"{IEM_ASOS}?station=LGA&data=tmpf&format=onlycomma"
                       f"&year1={end.year}&month1={end.month}&day1={end.day}"
                       f"&year2={end.year}&month2={end.month}&day2={end.day}")):
        try:
            http_get(url, attempts=2)
            print(f"  connection check {name}: OK")
        except Exception as exc:
            print(f"  connection check {name}: FAILED -> {exc}")
    for city, (station, lat, lon, tz_name) in sorted(chosen.items()):
        tz = ZoneInfo(tz_name)
        point = station_coordinates(db, city) if cfg.get("forecast_at_station", True) else None
        if point:
            lat, lon = point[0], point[1]
        print(f"{city} ({station}) at {lat:.3f},{lon:.3f}", flush=True)
        try:
            actual = station_daily_highs(station, start, end, tz)
        except Exception as exc:  # one bad city must not stop the rest
            print(f"    station history failed: {exc}")
            actual = {}
        if not actual:
            print("    no station history found; skipped")
            summary.append((city, station, "-", {}, "no station data"))
            continue
        now = datetime.now(timezone.utc).isoformat()
        for model in models:
            try:
                forecasts = forecast_daily_highs(model, lat, lon, tz_name, start, end)
            except Exception as exc:
                print(f"    {model}: forecast history failed: {exc}")
                summary.append((city, station, model, {}, f"forecast failed: {exc}"[:60]))
                continue
            rows = []
            stats = {}
            for lead, per_day in forecasts.items():
                residuals = []
                for day, forecast_c in per_day.items():
                    if day in actual:
                        residual = actual[day] - forecast_c
                        residuals.append(residual)
                        rows.append((city, station, model, lead, day, forecast_c, actual[day], residual, now))
                if len(residuals) >= 10:
                    stats[lead] = (len(residuals), mean(residuals), pstdev(residuals))
            with db:
                db.executemany("INSERT OR REPLACE INTO station_residuals VALUES (?,?,?,?,?,?,?,?,?)", rows)
            summary.append((city, station, model, stats, ""))
            time.sleep(0.5)

    print("\n=== FORECAST ERROR BY AIRPORT (degrees C; bias = actual - forecast) ===")
    print(f"{'city':<16}{'model':<14}" + "".join(f"{'D-' + str(l) + ' n/bias/sd':>20}" for l in LEADS))
    for city, station, model, stats, note in summary:
        cells = "".join(
            f"{stats[l][0]:>6}/{stats[l][1]:+5.1f}/{stats[l][2]:4.1f}  " if l in stats else f"{'-':>20}"
            for l in LEADS)
        print(f"{city[:15]:<16}{model[:13]:<14}{cells}{note}")


if __name__ == "__main__":
    main()
