"""Backfill weather-only history used to calibrate ECMWF/GFS/ICON for one live event."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as dt_time, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import truststore

from collector import DDL, station_coordinates
from probability import DEFAULT_SIGMA_FLOOR_C, calibration, event_markets, parse_time

truststore.inject_into_ssl()

AWC_METAR = "https://aviationweather.gov/api/data/metar"
OPEN_METEO_SINGLE = "https://single-runs-api.open-meteo.com/v1/forecast"
MODELS = ("ecmwf_ifs025", "gfs_global", "icon_global")
SOURCE = "calibration_backfill"


def fetch_json(url: str, attempts: int = 4, timeout: int = 60):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "polymarket-weather-calibration/1",
                    "Connection": "close",
                },
            )
            with urlopen(request, timeout=timeout) as response:
                if response.status == 204:
                    return []
                return json.load(response)
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(f"Request failed after {attempts} attempts: {url}\n{last_error}")


def parse_observation_time(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc)
    text = str(value or "").strip()
    if not text:
        raise ValueError("missing observation time")
    try:
        return datetime.fromtimestamp(float(text), timezone.utc)
    except ValueError:
        return parse_time(text)


def fetch_metar_window(station: str, start: datetime, end: datetime) -> list[dict]:
    """Fetch one historical window. AWC's date is the end time and hours looks backward."""
    hours = max(1, math.ceil((end - start).total_seconds() / 3600))
    params = urlencode({
        "ids": station,
        "format": "json",
        "hours": hours,
        "date": end.astimezone(timezone.utc).strftime("%Y%m%d_%H%M"),
    })
    payload = fetch_json(f"{AWC_METAR}?{params}")
    if not isinstance(payload, list):
        raise RuntimeError("AviationWeather returned an unexpected METAR payload")
    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        try:
            observed = parse_observation_time(item.get("obsTime") or item.get("reportTime"))
        except (TypeError, ValueError, OSError):
            continue
        if start <= observed < end:
            rows.append(item)
    return rows


def historical_metars(station: str, start: datetime, end: datetime,
                      chunk_hours: int = 72) -> list[dict]:
    rows: dict[str, dict] = {}
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(hours=chunk_hours), end)
        print(
            f"METAR {station} | {cursor.isoformat()} -> {chunk_end.isoformat()}",
            flush=True,
        )
        for item in fetch_metar_window(station, cursor, chunk_end):
            try:
                observed = parse_observation_time(item.get("obsTime") or item.get("reportTime"))
            except (TypeError, ValueError, OSError):
                continue
            key = str(item.get("rawOb") or observed.isoformat())
            rows[key] = item
        cursor = chunk_end
        if cursor < end:
            time.sleep(0.25)
    return list(rows.values())


def complete_days(payload: list[dict], tz: ZoneInfo, before: date,
                  min_readings: int) -> dict[str, list[tuple[datetime, float, dict]]]:
    grouped: dict[str, dict[str, tuple[datetime, float, dict]]] = {}
    for item in payload:
        try:
            observed = parse_observation_time(item.get("obsTime") or item.get("reportTime"))
            temp_c = float(item["temp"])
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if not math.isfinite(temp_c):
            continue
        local_day = observed.astimezone(tz).date()
        if local_day >= before:
            continue
        key = local_day.isoformat()
        grouped.setdefault(key, {})[observed.isoformat()] = (observed, temp_c, item)
    return {
        day: sorted(readings.values(), key=lambda row: row[0])
        for day, readings in grouped.items()
        if len(readings) >= min_readings
    }


def save_observations(db: sqlite3.Connection, city: str, station: str,
                      days: dict[str, list[tuple[datetime, float, dict]]],
                      fetched_at: str) -> int:
    inserted = 0
    for day_rows in days.values():
        for observed, temp_c, raw in day_rows:
            source_id = str(raw.get("rawOb") or f"{station}:{observed.timestamp():.0f}")
            cursor = db.execute(
                """INSERT INTO calibration_observations(
                       station,city,observed_at,temperature_c,source_url,
                       source_record_id,fetched_at,raw_json
                   ) VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(station,city,observed_at) DO UPDATE SET
                       temperature_c=excluded.temperature_c,
                       source_url=excluded.source_url,
                       source_record_id=excluded.source_record_id,
                       fetched_at=excluded.fetched_at,
                       raw_json=excluded.raw_json""",
                (
                    station,
                    city,
                    observed.isoformat(),
                    temp_c,
                    AWC_METAR,
                    source_id,
                    fetched_at,
                    json.dumps(raw, separators=(",", ":")),
                ),
            )
            if cursor.rowcount:
                inserted += 1
    return inserted


def floor_six_hours(moment: datetime) -> datetime:
    moment = moment.astimezone(timezone.utc)
    return moment.replace(
        hour=(moment.hour // 6) * 6,
        minute=0,
        second=0,
        microsecond=0,
    )


def comparable_decision(target_day: date, current_target: date,
                        current_local: datetime, tz: ZoneInfo) -> datetime:
    lead_days = max(0, (current_target - current_local.date()).days)
    decision_day = target_day - timedelta(days=lead_days)
    local_clock = current_local.timetz().replace(tzinfo=None)
    return datetime.combine(decision_day, local_clock, tz).astimezone(timezone.utc)


def import_comparable_forecasts(
    db: sqlite3.Connection,
    city: str,
    target_day: date,
    current_target: date,
    current_local: datetime,
    latitude: float,
    longitude: float,
    timezone_name: str,
    availability_lag_hours: float,
) -> int:
    decision = comparable_decision(target_day, current_target, current_local, ZoneInfo(timezone_name))
    run = floor_six_hours(decision - timedelta(hours=availability_lag_hours))
    fetched_at = run + timedelta(hours=availability_lag_hours)
    inserted = 0
    for model in MODELS:
        marker = f"{SOURCE}:{city}:{target_day.isoformat()}:{run.isoformat()}:{model}"
        old = [
            row[0]
            for row in db.execute(
                "SELECT id FROM forecast_batches WHERE endpoint=?",
                (marker,),
            )
        ]
        for batch_id in old:
            db.execute("DELETE FROM forecast_values WHERE batch_id=?", (batch_id,))
            db.execute("DELETE FROM forecast_batches WHERE id=?", (batch_id,))
        params = urlencode({
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m",
            "models": model,
            "run": run.strftime("%Y-%m-%dT%H:%M"),
            "forecast_days": 10,
            "timezone": timezone_name,
            "temperature_unit": "celsius",
        })
        payload = fetch_json(f"{OPEN_METEO_SINGLE}?{params}")
        hourly = payload.get("hourly") or {}
        values = []
        for stamp, value in zip(
            hourly.get("time", []),
            hourly.get("temperature_2m", []),
        ):
            if value is None or not str(stamp).startswith(target_day.isoformat()):
                continue
            values.append(float(value))
        if not values:
            raise RuntimeError(
                f"{model} run {run.isoformat()} has no forecast for {target_day}"
            )
        cursor = db.execute(
            """INSERT INTO forecast_batches(
                   city,model,fetched_at,model_run_at,endpoint,raw_json
               ) VALUES (?,?,?,?,?,?)""",
            (
                city,
                model,
                fetched_at.isoformat(),
                run.isoformat(),
                marker,
                json.dumps(payload, separators=(",", ":")),
            ),
        )
        db.execute(
            "INSERT INTO forecast_values VALUES (?,?,?,?)",
            (cursor.lastrowid, "daily_max", target_day.isoformat(), max(values)),
        )
        inserted += 1
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--history-days", type=int, default=15)
    parser.add_argument("--min-readings-per-day", type=int, default=18)
    parser.add_argument("--chunk-hours", type=int, default=72)
    parser.add_argument("--availability-lag-hours", type=float, default=6.0)
    parser.add_argument("--as-of", default=datetime.now(timezone.utc).isoformat())
    args = parser.parse_args()
    if args.days < 2:
        parser.error("--days must be >= 2")
    if args.history_days < args.days:
        parser.error("--history-days must be >= --days")
    if args.history_days > 15:
        parser.error("AviationWeather's rolling archive is limited to 15 days")
    if args.min_readings_per_day < 1:
        parser.error("--min-readings-per-day must be positive")
    if not 1 <= args.chunk_hours <= 96:
        parser.error("--chunk-hours must be between 1 and 96")
    if args.availability_lag_hours < 0:
        parser.error("--availability-lag-hours cannot be negative")

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
    if not db_path.is_file():
        parser.error(f"Collector database does not exist: {db_path}")

    as_of = parse_time(args.as_of)
    with sqlite3.connect(db_path) as db:
        db.executescript(DDL)
        bins, city, target, unit, station, _ = event_markets(db, args.event_id)
        if not station:
            parser.error(f"Event {args.event_id} has no parsed resolution station")
        location = db.execute(
            "SELECT latitude,longitude,timezone FROM locations WHERE city=?",
            (city,),
        ).fetchone()
        if not location:
            parser.error(f"No saved location/timezone for {city}")
        latitude, longitude, timezone_name = float(location[0]), float(location[1]), str(location[2])
        # Keep calibration forecasts at the same point the live collector uses.
        if cfg.get("forecast_at_station", True):
            station_point = station_coordinates(db, city)
            if station_point:
                latitude, longitude = station_point[0], station_point[1]
        tz = ZoneInfo(timezone_name)
        target_day = date.fromisoformat(target)
        current_local = as_of.astimezone(tz)

        history_end_local = datetime.combine(target_day, dt_time.min, tz)
        history_start_local = history_end_local - timedelta(days=args.history_days)
        start_utc = history_start_local.astimezone(timezone.utc)
        end_utc = history_end_local.astimezone(timezone.utc)

        print(
            f"Calibration target: {city} | station {station} | event {args.event_id}",
            flush=True,
        )
        print(
            f"Historical observations: {history_start_local.date()} -> "
            f"{history_end_local.date() - timedelta(days=1)}",
            flush=True,
        )

        metars = historical_metars(
            station,
            start_utc,
            end_utc,
            args.chunk_hours,
        )
        days = complete_days(
            metars,
            tz,
            target_day,
            args.min_readings_per_day,
        )
        selected_dates = sorted(days, reverse=True)[:args.days]
        selected_dates.sort()
        if len(selected_dates) < args.days:
            parser.error(
                f"Only {len(selected_dates)} complete METAR days found; "
                f"need {args.days}. Try a smaller --days value or rerun later."
            )
        selected = {day: days[day] for day in selected_dates}
        fetched_at = as_of.isoformat()
        with db:
            obs_count = save_observations(
                db, city, station, selected, fetched_at
            )

        print(
            f"Complete METAR days selected: {len(selected_dates)} | "
            f"rows stored: {obs_count}",
            flush=True,
        )
        for day in selected_dates:
            rows = selected[day]
            high_c = max(row[1] for row in rows)
            print(
                f"  {day} | readings={len(rows)} | observed_high_c={high_c:.2f}",
                flush=True,
            )

        forecast_count = 0
        for index, day_text in enumerate(selected_dates, 1):
            day = date.fromisoformat(day_text)
            print(
                f"Forecast history [{index}/{len(selected_dates)}] {day_text}",
                flush=True,
            )
            with db:
                forecast_count += import_comparable_forecasts(
                    db,
                    city,
                    day,
                    target_day,
                    current_local,
                    latitude,
                    longitude,
                    timezone_name,
                    args.availability_lag_hours,
                )

        sigma_floor = DEFAULT_SIGMA_FLOOR_C if unit == "C" else DEFAULT_SIGMA_FLOOR_C * 9 / 5
        fitted = calibration(
            db,
            city,
            station,
            unit,
            target,
            as_of,
            tz,
            args.days,
            sigma_floor,
        )
        print(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "event_id": args.event_id,
                    "city": city,
                    "station": station,
                    "unit": unit,
                    "complete_days": len(selected_dates),
                    "observation_rows": obs_count,
                    "forecast_batches": forecast_count,
                    "calibration": fitted,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
