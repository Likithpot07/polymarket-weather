"""Read-only weather market recorder. Python 3.11+."""
from __future__ import annotations
import argparse
import asyncio
import csv
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import re
import sqlite3
import time
import socket
import sys
from http.client import IncompleteRead
import dns.resolver
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
LOG = logging.getLogger("collector")
# App-only DNS override for Gamma and the CLOB book stream.
# Windows DNS settings are untouched.
_APP_DNS_HOSTS = {
    "gamma-api.polymarket.com",
    "ws-subscriptions-clob.polymarket.com",
    "clob.polymarket.com",
    "data-api.polymarket.com",
}
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
def _cloudflare_app_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host in _APP_DNS_HOSTS:
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = ["1.1.1.1", "1.0.0.1"]
        resolver.lifetime = 5.0
        addresses = []
        # Use IPv4 addresses from Cloudflare for these Polymarket hosts.
        try:
            addresses.extend(str(r) for r in resolver.resolve(host, "A"))
        except Exception:
            pass
        if not addresses:
            raise socket.gaierror(f"Cloudflare DNS could not resolve {host}")
        results = []
        for ip in addresses:
            results.extend(_ORIGINAL_GETADDRINFO(ip, port, socket.AF_INET, type, proto, flags))
        return results
    return _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)
socket.getaddrinfo = _cloudflare_app_getaddrinfo
GAMMA = "https://gamma-api.polymarket.com"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
AVIATION_WEATHER = "https://aviationweather.gov/api/data/metar"
DDL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS events (
 id TEXT PRIMARY KEY, slug TEXT, title TEXT, raw_json TEXT NOT NULL, refreshed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS markets (
 id TEXT PRIMARY KEY, event_id TEXT NOT NULL REFERENCES events(id), slug TEXT,
 question TEXT, description TEXT, condition_id TEXT, city TEXT, target_date TEXT,
 active INTEGER NOT NULL, raw_json TEXT NOT NULL, refreshed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tokens (
 token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL REFERENCES markets(id), side TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rule_reviews (
 market_id TEXT PRIMARY KEY REFERENCES markets(id), station TEXT, source_url TEXT,
 unit TEXT, rounding TEXT, time_window TEXT, verified INTEGER NOT NULL DEFAULT 0,
 notes TEXT, reviewed_at TEXT);
CREATE TABLE IF NOT EXISTS locations (
 city TEXT PRIMARY KEY, latitude REAL NOT NULL, longitude REAL NOT NULL,
 timezone TEXT NOT NULL, source TEXT NOT NULL, raw_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS forecast_batches (
 id INTEGER PRIMARY KEY, city TEXT NOT NULL, model TEXT NOT NULL,
 fetched_at TEXT NOT NULL, model_run_at TEXT, endpoint TEXT NOT NULL,
 raw_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS forecast_values (
 batch_id INTEGER NOT NULL REFERENCES forecast_batches(id), period TEXT NOT NULL,
 valid_time TEXT NOT NULL, temperature_c REAL NOT NULL,
 PRIMARY KEY(batch_id,period,valid_time));
CREATE TABLE IF NOT EXISTS book_events (
 id INTEGER PRIMARY KEY, token_id TEXT, event_type TEXT NOT NULL,
 source_timestamp TEXT, received_at TEXT NOT NULL, best_bid TEXT, best_ask TEXT,
 raw_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS book_token_time ON book_events(token_id,received_at);
CREATE INDEX IF NOT EXISTS forecast_city_time ON forecast_batches(city,fetched_at);
CREATE TABLE IF NOT EXISTS observations (
 id INTEGER PRIMARY KEY, market_id TEXT NOT NULL REFERENCES markets(id),
 station TEXT NOT NULL, observed_at TEXT NOT NULL, temperature REAL NOT NULL,
 unit TEXT NOT NULL, source_url TEXT NOT NULL, source_record_id TEXT,
 fetched_at TEXT NOT NULL, raw_json TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS observation_unique
ON observations(market_id,station,observed_at,source_record_id);
CREATE TABLE IF NOT EXISTS calibration_observations (
 station TEXT NOT NULL,
 city TEXT NOT NULL,
 observed_at TEXT NOT NULL,
 temperature_c REAL NOT NULL,
 source_url TEXT NOT NULL,
 source_record_id TEXT,
 fetched_at TEXT NOT NULL,
 raw_json TEXT NOT NULL,
 PRIMARY KEY(station,city,observed_at));
CREATE INDEX IF NOT EXISTS calibration_obs_station_time
ON calibration_observations(station,city,observed_at);
"""
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
def get_json(url: str, retries: int = 5) -> dict | list:
    """Fetch JSON and retry transient/truncated HTTP responses."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(
                url,
                headers={
                    "User-Agent": "weather-collector/0.1",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            with urlopen(request, timeout=25) as response:
                raw = response.read()
            return json.loads(raw.decode("utf-8"))
        except HTTPError as exc:
            # 4xx (e.g. 404 for a slug that doesn't exist yet) won't fix itself on retry.
            if 400 <= exc.code < 500 and exc.code != 429:
                raise
            last_error = exc
        except (IncompleteRead, URLError, TimeoutError, ConnectionResetError, OSError) as exc:
            last_error = exc
        except json.JSONDecodeError as exc:
            last_error = exc
        if attempt < retries:
            delay = min(2 ** (attempt - 1), 10)
            LOG.warning(
                "HTTP/JSON read failed | attempt %d/%d | %s | %s | retrying in %ss",
                attempt,
                retries,
                url,
                last_error,
                delay,
            )
            time.sleep(delay)
    raise URLError(f"JSON request failed after {retries} attempts: {url}") from last_error
def db_open(path: str) -> sqlite3.Connection:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Wait up to 60s for other scripts (live.py, backtests) instead of crashing.
    db = sqlite3.connect(target, timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(DDL)
    return db
def as_list(value) -> list:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []
def extract_city(title: str) -> str | None:
    m = re.search(r"(?:highest|high|maximum)\s+temperature\s+in\s+(.+?)\s+(?:on|for)\b", title, re.I)
    return m.group(1).strip(" ?") if m else None
def extract_date(title: str, slug: str) -> str | None:
    combined = f"{title} {slug}"
    iso = re.search(r"\b(20\d\d)-(\d\d)-(\d\d)\b", combined)
    if iso:
        try:
            return datetime.strptime(iso.group(), "%Y-%m-%d").date().isoformat()
        except ValueError:
            pass
    for month, day, year in re.findall(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)[\s-]+(\d{1,2})(?:,?[\s-]+(20\d\d))\b",
        combined, re.I):
        try:
            return datetime.strptime(f"{month} {day} {year}", "%B %d %Y").date().isoformat()
        except ValueError:
            pass
    return None
def candidate_event(event: dict) -> bool:
    title = str(event.get("title") or "")
    return bool(re.search(r"(?:highest|high|maximum)\s+temperature\s+in\s+.+?\s+(?:on|for)\b", title, re.I))
def parse_rule_metadata(
    description: str,
    resolution_source: str | None,
    target_date: str | None,
) -> dict[str, str | None]:
    """Extract resolution URL, ICAO station, unit, and precision from market rules."""
    text = " ".join(
        part for part in (description or "", resolution_source or "") if part
    )
    source_url = None
    # Current Polymarket weather markets may resolve from NOAA weather.gov.
    noaa_match = re.search(
        r'https?://(?:www\.)?weather\.gov/wrh/timeseries\\?[^\s)>"]*site=[A-Za-z0-9]+[^\s)>"]*',
        text,
        re.I,
    )
    # Some markets use Weather Underground Daily Observations.
    wu_match = re.search(
        r'https?://(?:www\.)?wunderground\.com/history/daily/[^\s)>"]+',
        text,
        re.I,
    )
    if noaa_match:
        source_url = noaa_match.group(0).rstrip(".,;")
    elif wu_match:
        source_url = wu_match.group(0).rstrip(".,;")
    elif resolution_source:
        source_url = str(resolution_source).rstrip(".,;")
    station = None
    # NOAA URLs encode the ICAO station as ?site=KLGA.
    if source_url:
        site_match = re.search(r"[?&]site=([A-Za-z0-9]{4,6})\b", source_url, re.I)
        if site_match:
            station = site_match.group(1).upper()
    # Wunderground URLs normally end with the ICAO station, e.g. /RCSS.
    if not station and source_url and "wunderground.com/history/daily/" in source_url.lower():
        candidate = source_url.rstrip("/").split("/")[-1].split("?")[0].upper()
        if re.fullmatch(r"[A-Z0-9]{4,6}", candidate):
            station = candidate
    unit = None
    # The settlement clause ("whole degrees Celsius") is the most reliable signal;
    # rule text sometimes mentions the other scale elsewhere.
    whole = re.search(r"whole\s+degrees?\s+(Fahrenheit|Celsius)", text, re.I)
    if whole:
        unit = whole.group(1)[0].upper()
    elif re.search(r"degrees?\s+Fahrenheit|°F", text, re.I):
        unit = "F"
    elif re.search(r"degrees?\s+Celsius|°C", text, re.I):
        unit = "C"
    rounding = "whole degree" if re.search(r"whole\s+degrees?", text, re.I) else None
    return {
        "station": station,
        "source_url": source_url,
        "unit": unit,
        "rounding": rounding,
        "time_window": target_date,
    }
def save_event(db: sqlite3.Connection, event: dict) -> dict[str, str]:
    title = str(event.get("title") or "")
    city = extract_city(title)
    now = utc_now()
    eid = str(event["id"])
    db.execute("INSERT INTO events VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET slug=excluded.slug,title=excluded.title,raw_json=excluded.raw_json,refreshed_at=excluded.refreshed_at",
               (eid, event.get("slug"), title, json.dumps(event), now))
    token_map: dict[str, str] = {}
    for market in event.get("markets") or []:
        mid = str(market["id"])
        question = market.get("question") or ""
        description = market.get("description") or event.get("description") or ""
        target = extract_date(title, str(event.get("slug") or ""))
        db.execute("""INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET question=excluded.question,description=excluded.description,
                    condition_id=excluded.condition_id,city=excluded.city,target_date=excluded.target_date,
                    active=excluded.active,raw_json=excluded.raw_json,refreshed_at=excluded.refreshed_at""",
                   (mid, eid, market.get("slug"), question, description, market.get("conditionId"),
                    city, target, int(bool(market.get("active", True)) and not bool(market.get("closed"))),
                    json.dumps(market), now))
        db.execute("INSERT OR IGNORE INTO rule_reviews(market_id) VALUES (?)", (mid,))
        resolution_source = (
            market.get("resolutionSource")
            or event.get("resolutionSource")
            or ""
        )
        rule = parse_rule_metadata(
            description,
            str(resolution_source),
            target,
        )
        db.execute(
            """UPDATE rule_reviews
               SET station=COALESCE(station,?),
                   source_url=COALESCE(source_url,?),
                   unit=COALESCE(unit,?),
                   rounding=COALESCE(rounding,?),
                   time_window=COALESCE(time_window,?),
                   notes=COALESCE(notes,'Auto-parsed from Polymarket market rules')
               WHERE market_id=?""",
            (
                rule["station"],
                rule["source_url"],
                rule["unit"],
                rule["rounding"],
                rule["time_window"],
                mid,
            ),
        )
        ids = as_list(market.get("clobTokenIds"))
        outcomes = as_list(market.get("outcomes"))
        if len(ids) == 2 and [str(s).lower() for s in outcomes] == ["yes", "no"]:
            for token, side in zip(ids, ("YES", "NO")):
                token = str(token)
                db.execute("INSERT INTO tokens VALUES (?,?,?) ON CONFLICT(token_id) DO UPDATE SET market_id=excluded.market_id,side=excluded.side", (token, mid, side))
                if market.get("active", True) and not market.get("closed"):
                    token_map[token] = mid
        else:
            LOG.warning("Market %s has unrecognized outcome/token mapping; skipping book", mid)
    db.commit()
    LOG.info("EVENT %s | %s | %s | %s markets, %s tokens", eid, title, city or "city unknown", len(event.get("markets") or []), len(token_map))
    return token_map
async def fetch(url: str) -> dict | list:
    return await asyncio.to_thread(get_json, url)
SLUG_PATTERN = re.compile(r"^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-20\d\d$")
_MISSING_SLUGS: dict[str, float] = {}


def upcoming_slugs(db: sqlite3.Connection, days_ahead: int, today=None) -> list[str]:
    """Slugs for every known city for today..today+days_ahead that we haven't saved yet.

    Search results are capped and favour busy markets, so newly listed events a few
    days out were often missed; those early markets are where a forecast can beat
    the price. Polymarket names these events predictably, so ask for them directly.
    """
    today = today or datetime.now(timezone.utc).date()
    known = {str(row[0]) for row in db.execute("SELECT slug FROM events WHERE slug IS NOT NULL")}
    cities = sorted({m.group(1) for s in known if (m := SLUG_PATTERN.match(s))})
    slugs = []
    for offset in range(-1, days_ahead + 1):
        day = today + timedelta(days=offset)
        suffix = f"on-{day.strftime('%B').lower()}-{day.day}-{day.year}"
        for city in cities:
            slug = f"highest-temperature-in-{city}-{suffix}"
            if slug not in known:
                slugs.append(slug)
    return slugs


async def scan(db: sqlite3.Connection, cfg: dict) -> dict[str, str]:
    token_map: dict[str, str] = {}
    seen: set[str] = set()
    # Pin requested events ahead of search results and cached markets so that
    # max_tokens cannot silently exclude an event being evaluated.
    pinned_ids = list(dict.fromkeys(str(eid) for eid in cfg.get("event_ids", [])))
    for eid in pinned_ids:
        try:
            detail = await fetch(f"{GAMMA}/events/{quote(eid)}")
            if str(detail["id"]) != eid:
                raise ValueError(f"Gamma returned a different event for {eid}")
            seen.add(eid)
            token_map.update(save_event(db, detail))
        except (HTTPError, URLError, ValueError, KeyError, TypeError) as exc:
            LOG.error("Pinned Gamma event %s failed: %s", eid, exc)
        # A previously saved event remains subscribed if its refresh fails.
        for token, mid in db.execute("""SELECT t.token_id,t.market_id FROM tokens t
                JOIN markets m ON t.market_id=m.id WHERE m.event_id=? AND m.active=1""", (eid,)):
            token_map.setdefault(token, mid)
    pinned_count = len(token_map)
    for term in cfg["search_terms"]:
        try:
            result = await fetch(f"{GAMMA}/public-search?{urlencode({'q': term, 'limit_per_type': cfg['max_events_per_search']})}")
            hits = result.get("events", [])
        except (HTTPError, URLError, ValueError) as exc:
            LOG.error("Gamma search %r failed: %s", term, exc)
            continue
        for item in hits[:cfg["max_events_per_search"]]:
            if not candidate_event(item):
                continue
            eid = str(item["id"])
            if eid in seen:
                continue
            seen.add(eid)
            try:
                detail = await fetch(f"{GAMMA}/events/{quote(eid)}")
                token_map.update(save_event(db, detail))
            except (HTTPError, URLError, ValueError, KeyError, TypeError) as exc:
                LOG.error("Gamma event %s failed: %s", eid, exc)
    found = 0
    for slug in upcoming_slugs(db, int(cfg.get("discovery_days_ahead", 3))):
        if time.monotonic() - _MISSING_SLUGS.get(slug, -1e9) < 3600:
            continue
        try:
            detail = await fetch(f"{GAMMA}/events/slug/{quote(slug, safe='')}")
            if isinstance(detail, dict) and detail.get("id") and candidate_event(detail):
                if str(detail["id"]) not in seen:
                    seen.add(str(detail["id"]))
                    token_map.update(save_event(db, detail))
                    found += 1
            else:
                _MISSING_SLUGS[slug] = time.monotonic()
        except (HTTPError, URLError, ValueError, KeyError, TypeError):
            _MISSING_SLUGS[slug] = time.monotonic()
    if found:
        LOG.info("Early discovery: %d new events found by slug", found)
    for slug in cfg["event_slugs"]:
        try:
            detail = await fetch(f"{GAMMA}/events/slug/{quote(slug, safe='')}")
            if str(detail["id"]) not in seen:
                seen.add(str(detail["id"]))
                token_map.update(save_event(db, detail))
        except (HTTPError, URLError, ValueError, KeyError, TypeError) as exc:
            LOG.error("Gamma slug %s failed: %s", slug, exc)
    # Keep subscriptions to markets already stored when a search page misses them.
    for token, mid in db.execute("SELECT t.token_id,t.market_id FROM tokens t JOIN markets m ON t.market_id=m.id WHERE m.active=1"):
        token_map.setdefault(token, mid)
    LOG.info("CLOB tokens discovered: %d (pinned: %d)", len(token_map), pinned_count)
    return token_map
async def location_for(db: sqlite3.Connection, cfg: dict, city: str) -> dict | None:
    configured = cfg.get("city_locations", {}).get(city)
    if configured:
        data = dict(configured)
        source = "config"
    else:
        row = db.execute("SELECT latitude,longitude,timezone FROM locations WHERE city=?", (city,)).fetchone()
        if row:
            return dict(zip(("latitude", "longitude", "timezone"), row))
        result = await fetch(f"{GEOCODE}?{urlencode({'name': city, 'count': 1, 'language': 'en'})}")
        hits = result.get("results") or []
        if not hits:
            LOG.warning("No coordinates for %s; add city_locations override", city)
            return None
        data = hits[0]
        source = "open-meteo-geocoding"
    if not all(k in data for k in ("latitude", "longitude", "timezone")):
        raise ValueError(f"Incomplete location for {city}")
    db.execute("INSERT INTO locations VALUES (?,?,?,?,?,?) ON CONFLICT(city) DO UPDATE SET latitude=excluded.latitude,longitude=excluded.longitude,timezone=excluded.timezone,source=excluded.source,raw_json=excluded.raw_json",
               (city, data["latitude"], data["longitude"], data["timezone"], source, json.dumps(data)))
    db.commit()
    return data
_STATION_CACHE: dict[str, tuple[float, float, str]] = {}


def station_coordinates(db: sqlite3.Connection, city: str) -> tuple[float, float, str] | None:
    """Lat/lon of the airport station that settles this city's markets.

    Markets resolve on one airport (e.g. NYC -> LaGuardia), which can be several
    degrees warmer/cooler than the geocoded city centre. Coordinates come from the
    METAR payloads already stored, so nothing is hard-coded or guessed.
    """
    if city in _STATION_CACHE:
        return _STATION_CACHE[city]
    # Fast path: one recent market for the city, then its latest observation via the
    # (market_id, ...) index. The old join scanned the whole observations table.
    row = None
    for market_id, station in db.execute(
            """SELECT m.id,r.station FROM markets m JOIN rule_reviews r ON r.market_id=m.id
               WHERE m.city=? AND r.station IS NOT NULL ORDER BY m.target_date DESC LIMIT 5""",
            (city,)):
        hit = db.execute(
            "SELECT raw_json FROM observations WHERE market_id=? AND station=? ORDER BY observed_at DESC LIMIT 1",
            (market_id, station)).fetchone()
        if hit:
            row = (station, hit[0])
            break
    if not row:
        return None
    try:
        raw = json.loads(row[1] or "{}")
        lat, lon = float(raw["lat"]), float(raw["lon"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    _STATION_CACHE[city] = (lat, lon, str(row[0]))
    return _STATION_CACHE[city]


async def collect_forecasts(db: sqlite3.Connection, cfg: dict) -> None:
    cities = [row[0] for row in db.execute("SELECT DISTINCT city FROM markets WHERE city IS NOT NULL AND active=1")]
    for city in cities:
        try:
            loc = await location_for(db, cfg, city)
        except (HTTPError, URLError, ValueError) as exc:
            LOG.error("Geocode %s: %s", city, exc)
            continue
        if not loc:
            continue
        latitude, longitude = loc["latitude"], loc["longitude"]
        if cfg.get("forecast_at_station", True):
            station = station_coordinates(db, city)
            if station:
                latitude, longitude, _ = station
        for model in cfg["weather_models"]:
            params = dict(latitude=latitude, longitude=longitude,
                          timezone=loc["timezone"], models=model, hourly="temperature_2m",
                          daily="temperature_2m_max", temperature_unit="celsius",
                          forecast_days=cfg["forecast_days"])
            url = f"{OPEN_METEO}?{urlencode(params)}"
            try:
                payload = await fetch(url)
                if payload.get("error"):
                    raise ValueError(payload.get("reason", "API error"))
                hourly = payload.get("hourly") or {}
                daily = payload.get("daily") or {}
                if not hourly.get("time") or not hourly.get("temperature_2m"):
                    raise ValueError("missing hourly temperature_2m")
                received = utc_now()
                with db:
                    cursor = db.execute("INSERT INTO forecast_batches(city,model,fetched_at,model_run_at,endpoint,raw_json) VALUES (?,?,?,NULL,?,?)",
                                        (city, model, received, url, json.dumps(payload)))
                    for period, data, key in (("hourly", hourly, "temperature_2m"), ("daily_max", daily, "temperature_2m_max")):
                        for valid, value in zip(data.get("time", []), data.get(key, []), strict=True):
                            if value is not None:
                                if period == "hourly":
                                    valid = datetime.fromisoformat(valid).replace(tzinfo=ZoneInfo(payload.get("timezone") or loc["timezone"])).isoformat()
                                db.execute("INSERT INTO forecast_values VALUES (?,?,?,?)", (cursor.lastrowid, period, valid, value))
                LOG.info("FORECAST %s %s | %d hourly values", city, model, len(hourly["time"]))
            except (HTTPError, URLError, ValueError) as exc:
                LOG.error("Forecast %s %s failed: %s", city, model, exc)
        if cfg.get("collect_nws", True):
            await collect_nws(db, city, latitude, longitude, loc["timezone"])


NWS_POINTS = "https://api.weather.gov/points"
_NWS_GRID: dict[str, str] = {}


def nws_daily_highs(grid: dict, timezone_name: str) -> dict[str, float]:
    """{local_date: forecast high C} from an api.weather.gov gridpoint payload."""
    tz = ZoneInfo(timezone_name)
    out: dict[str, float] = {}
    for item in ((grid.get("properties") or {}).get("maxTemperature") or {}).get("values") or []:
        try:
            start = datetime.fromisoformat(str(item["validTime"]).split("/")[0])
            value = float(item["value"])
        except (KeyError, TypeError, ValueError):
            continue
        day = start.astimezone(tz).date().isoformat()
        out[day] = max(value, out.get(day, value))
    return out


async def collect_nws(db: sqlite3.Connection, city: str, lat: float, lon: float, timezone_name: str) -> None:
    """US National Weather Service forecast high (human-adjusted), saved for later comparison only.

    Stored as model 'nws' with period 'nws_daily_max', so the live model ignores it
    until it has proven itself against our current forecasts.
    """
    station = station_coordinates(db, city)
    if not station or not station[2].startswith("K"):
        return  # US airports only
    try:
        if city not in _NWS_GRID:
            point = await fetch(f"{NWS_POINTS}/{lat:.4f},{lon:.4f}")
            _NWS_GRID[city] = str(point["properties"]["forecastGridData"])
        grid = await fetch(_NWS_GRID[city])
        highs = nws_daily_highs(grid, timezone_name)
        if not highs:
            raise ValueError("no maxTemperature values")
        with db:
            cursor = db.execute(
                "INSERT INTO forecast_batches(city,model,fetched_at,model_run_at,endpoint,raw_json) VALUES (?,?,?,?,?,?)",
                (city, "nws", utc_now(), (grid.get("properties") or {}).get("updateTime"), _NWS_GRID[city], "{}"))
            db.executemany("INSERT INTO forecast_values VALUES (?,?,?,?)",
                           [(cursor.lastrowid, "nws_daily_max", day, value) for day, value in highs.items()])
        LOG.info("FORECAST %s nws | %d days", city, len(highs))
    except (HTTPError, URLError, ValueError, KeyError, TypeError) as exc:
        LOG.warning("NWS forecast %s failed: %s", city, exc)


def convert_temperature(value_c: float, unit: str | None) -> tuple[float, str]:
    if str(unit or "").upper() == "F":
        return value_c * 9.0 / 5.0 + 32.0, "F"
    return value_c, "C"
async def collect_observations(db: sqlite3.Connection, cfg: dict) -> None:
    """
    Collect actual airport observations from AviationWeather METAR.
    Polymarket weather markets may resolve from NOAA weather.gov or
    Weather Underground. rule_reviews preserves the official resolution URL,
    while AviationWeather METAR supplies machine-readable observations from
    the same ICAO airport station.
    """
    rows = db.execute(
        """SELECT m.id, m.city, m.target_date,
                  rr.station, rr.unit, rr.source_url
           FROM markets m
           JOIN rule_reviews rr ON rr.market_id=m.id
           WHERE m.active=1
             AND m.target_date IS NOT NULL
             AND rr.station IS NOT NULL"""
    ).fetchall()
    if not rows:
        LOG.warning(
            "No observation stations parsed yet; check NOAA/Wunderground "
            "resolution URLs in the market rules"
        )
        return
    by_station: dict[str, list[tuple]] = {}
    for row in rows:
        by_station.setdefault(str(row[3]).upper(), []).append(row)
    hours = int(cfg.get("observation_hours", 48))
    total_inserted = 0
    for station, station_rows in sorted(by_station.items()):
        url = (
            f"{AVIATION_WEATHER}?"
            + urlencode(
                {
                    "ids": station,
                    "format": "json",
                    "hours": hours,
                }
            )
        )
        try:
            payload = await fetch(url)
        except (HTTPError, URLError, ValueError) as exc:
            LOG.error("OBS %s failed: %s", station, exc)
            continue
        if not isinstance(payload, list):
            LOG.warning("OBS %s returned unexpected payload", station)
            continue
        city = next((str(r[1]) for r in station_rows if r[1]), None)
        timezone_name = None
        if city:
            loc_row = db.execute(
                "SELECT timezone FROM locations WHERE city=?",
                (city,),
            ).fetchone()
            if loc_row:
                timezone_name = loc_row[0]
            if not timezone_name:
                try:
                    loc = await location_for(db, cfg, city)
                except (HTTPError, URLError, ValueError):
                    loc = None
                if loc:
                    timezone_name = loc.get("timezone")
        if not timezone_name:
            LOG.warning(
                "OBS %s skipped: timezone unavailable for %s",
                station,
                city or "unknown city",
            )
            continue
        try:
            local_tz = ZoneInfo(str(timezone_name))
        except Exception:
            LOG.warning(
                "OBS %s skipped: invalid timezone %s",
                station,
                timezone_name,
            )
            continue
        inserted = 0
        for observation in payload:
            if not isinstance(observation, dict):
                continue
            obs_epoch = observation.get("obsTime")
            try:
                obs_dt = datetime.fromtimestamp(
                    float(obs_epoch),
                    tz=timezone.utc,
                )
            except (TypeError, ValueError, OSError):
                continue
            local_date = obs_dt.astimezone(local_tz).date().isoformat()
            try:
                temp_c = float(observation.get("temp"))
            except (TypeError, ValueError):
                continue
            source_record_id = f"{station}:{int(float(obs_epoch))}"
            observed_at = obs_dt.isoformat()
            fetched_at = utc_now()
            raw_json = json.dumps(observation)
            for (
                market_id,
                _city,
                target_date,
                _station,
                unit,
                _resolution_url,
            ) in station_rows:
                if str(target_date) != local_date:
                    continue
                temperature, stored_unit = convert_temperature(temp_c, unit)
                cursor = db.execute(
                    """INSERT OR IGNORE INTO observations(
                           market_id,station,observed_at,temperature,unit,
                           source_url,source_record_id,fetched_at,raw_json
                       ) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        str(market_id),
                        station,
                        observed_at,
                        temperature,
                        stored_unit,
                        url,
                        source_record_id,
                        fetched_at,
                        raw_json,
                    ),
                )
                if cursor.rowcount and cursor.rowcount > 0:
                    inserted += cursor.rowcount
        if inserted:
            db.commit()
            total_inserted += inserted
        LOG.info(
            "OBS %s | %d METAR rows | %d new market observations",
            station,
            len(payload),
            inserted,
        )
    LOG.info("OBS collection complete | %d new rows", total_inserted)
def record_frame(db: sqlite3.Connection, frame: dict, tokens: set[str]) -> int:
    """Append only; never synthesize book depth from price changes."""
    event_type = str(frame.get("event_type") or frame.get("type") or "unknown")
    received = utc_now()
    payloads = frame.get("price_changes") if event_type == "price_change" else None
    entries = payloads if isinstance(payloads, list) else [frame]
    count = 0
    for entry in entries:
        token = str(entry.get("asset_id") or frame.get("asset_id") or "")
        if token not in tokens:
            continue
        bid = entry.get("best_bid")
        ask = entry.get("best_ask")
        if event_type == "book":
            bids, asks = frame.get("bids") or [], frame.get("asks") or []
            bid = max((float(level["price"]) for level in bids), default=None)
            ask = min((float(level["price"]) for level in asks), default=None)
        stored = frame
        if event_type == "price_change":
            stored = dict(frame)
            stored["price_changes"] = [entry]
        db.execute("INSERT INTO book_events(token_id,event_type,source_timestamp,received_at,best_bid,best_ask,raw_json) VALUES (?,?,?,?,?,?,?)",
                   (token, event_type, str(frame.get("timestamp") or ""), received,
                    str(bid) if bid is not None else None, str(ask) if ask is not None else None,
                    json.dumps(stored)))
        count += 1
    if count:
        db.commit()
    return count
def chunk_tokens(tokens: set[str], size: int) -> list[set[str]]:
    """Split tokens across websocket subscriptions without dropping any token."""
    if size < 1:
        raise ValueError("max_tokens_per_socket must be positive")
    ordered = sorted(tokens)
    return [set(ordered[i:i + size]) for i in range(0, len(ordered), size)]

async def stream_books(db: sqlite3.Connection, tokens: set[str], socket_id: int = 1) -> None:
    import websockets
    delay = 1
    while tokens:
        try:
            async with websockets.connect(WS, ping_interval=None, max_size=8_000_000) as ws:
                await ws.send(json.dumps({"assets_ids": sorted(tokens), "type": "market", "custom_feature_enabled": True}))
                LOG.info("CLOB socket %d connected | %d tokens", socket_id, len(tokens))
                delay = 1
                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")
                beat = asyncio.create_task(heartbeat())
                try:
                    async for message in ws:
                        if message in ("PONG", "PING"):
                            continue
                        try:
                            frames = json.loads(message)
                            for frame in frames if isinstance(frames, list) else [frames]:
                                if isinstance(frame, dict):
                                    record_frame(db, frame, tokens)
                        except (ValueError, TypeError, KeyError) as exc:
                            LOG.warning("Unrecognized CLOB frame: %s", exc)
                finally:
                    beat.cancel()
                    await asyncio.gather(beat, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("CLOB socket %d disconnected (%s); retrying in %ss", socket_id, exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

async def run_auto_prepare(cfg: dict) -> None:
    """Run slow rule verification/calibration in a separate process so collection never blocks."""
    script = Path(__file__).resolve().with_name("auto_prepare.py")
    config_path = str(cfg.get("_config_path") or Path(__file__).resolve().with_name("config.json"))
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        "--config",
        config_path,
    )
    try:
        code = await process.wait()
        if code:
            LOG.warning("AUTO PREP exited with code %s", code)
    except asyncio.CancelledError:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        raise

async def collect(
    db: sqlite3.Connection,
    cfg: dict,
    seconds: float | None,
    once: bool,
) -> None:
    ws_tasks: list[asyncio.Task] = []
    prep_task = None
    current: set[str] = set()
    last_forecast = 0.0
    last_observation = 0.0
    last_prepare = 0.0
    last_prune = 0.0
    prune_task = None
    try:
        while True:
            # 1. Discover active Polymarket weather markets/tokens.
            token_map = await scan(db, cfg)
            tokens = set(token_map)
            # Only stream prices for sides we trade (NO-only halves storage).
            allowed = _traded_sides(cfg)
            if tokens and allowed != {"YES", "NO"}:
                sides = dict(db.execute("SELECT token_id,side FROM tokens").fetchall())
                tokens = {t for t in tokens if str(sides.get(t, "")).upper() in allowed}
            if not tokens:
                LOG.warning(
                    "No tokens found; check Gamma connectivity or add event_slugs"
                )
            # Snapshot mode: metadata + observations + forecasts, then exit.
            if once:
                await collect_observations(db, cfg)
                await collect_forecasts(db, cfg)
                return
            # 2. Start/refresh CLOB websockets immediately.
            socket_size = int(cfg.get("max_tokens_per_socket", cfg.get("max_tokens", 200)))
            sockets_healthy = bool(ws_tasks) and all(not task.done() for task in ws_tasks)
            if tokens != current or not sockets_healthy:
                for task in ws_tasks:
                    task.cancel()
                if ws_tasks:
                    await asyncio.gather(*ws_tasks, return_exceptions=True)
                ws_tasks = []
                current = tokens
                if current:
                    chunks = chunk_tokens(current, socket_size)
                    LOG.info(
                        "CLOB subscribing to all %d tokens across %d socket(s), max %d/socket",
                        len(current),
                        len(chunks),
                        socket_size,
                    )
                    ws_tasks = [
                        asyncio.create_task(stream_books(db, chunk, index))
                        for index, chunk in enumerate(chunks, 1)
                    ]
            # Let the websocket task start before slower REST collection.
            await asyncio.sleep(0)
            now = time.monotonic()
            # 3. Collect actual airport observations.
            if (
                now - last_observation
                >= cfg.get("observation_interval_seconds", 600)
            ):
                await collect_observations(db, cfg)
                last_observation = time.monotonic()
            # 4. Collect weather-model forecasts.
            now = time.monotonic()
            if (
                now - last_forecast
                >= cfg["forecast_interval_seconds"]
            ):
                await collect_forecasts(db, cfg)
                last_forecast = time.monotonic()
            # 5. Verify rules + calibrate newly discovered events in a background process.
            now = time.monotonic()
            prepare_interval = float(cfg.get("prepare_interval_seconds", 900))
            if (
                cfg.get("auto_prepare", True)
                and (prep_task is None or prep_task.done())
                and now - last_prepare >= prepare_interval
            ):
                if prep_task is not None:
                    try:
                        await prep_task
                    except Exception as exc:
                        LOG.warning("AUTO PREP task failed: %s", exc)
                prep_task = asyncio.create_task(run_auto_prepare(cfg))
                last_prepare = time.monotonic()
            # 6. Delete old price history / decision detail in a background thread.
            now = time.monotonic()
            if (cfg.get("_db_path") and (prune_task is None or prune_task.done())
                    and now - last_prune >= float(cfg.get("prune_interval_hours", 6)) * 3600):
                if prune_task is not None:
                    try:
                        LOG.info("PRUNE done | deleted %s", prune_task.result())
                    except Exception as exc:
                        LOG.warning("PRUNE failed: %s", exc)
                prune_task = asyncio.create_task(asyncio.to_thread(prune_db, cfg["_db_path"], cfg))
                last_prune = time.monotonic()
            # Short test mode. Runtime starts after initial refreshes.
            if seconds is not None:
                LOG.info(
                    "Collector running for %.0f seconds",
                    seconds,
                )
                await asyncio.sleep(seconds)
                return
            # 6. Normal continuous mode.
            await asyncio.sleep(
                cfg["scan_interval_seconds"]
            )
    finally:
        if prune_task and not prune_task.done():
            LOG.info("Waiting for database cleanup to finish ...")
            await asyncio.gather(prune_task, return_exceptions=True)
        if prep_task:
            prep_task.cancel()
            await asyncio.gather(prep_task, return_exceptions=True)
        for task in ws_tasks:
            task.cancel()
        if ws_tasks:
            await asyncio.gather(*ws_tasks, return_exceptions=True)
# ---------------------------------------------------------------------------
# Storage limits. Price history (book_events) and per-bracket decision detail grow
# by ~10 GB/day if kept forever; the bot only needs the last few hours of prices.
# Config: keep_book_hours (12), keep_decision_days (7), prune_interval_hours (6).
# Never touched: bets, forecasts, observations, markets, results.
# ---------------------------------------------------------------------------
def _first_id_at_or_after(db: sqlite3.Connection, table: str, ts_col: str, cutoff: str) -> int | None:
    """Ids grow with time, so binary-search the first id whose timestamp >= cutoff.
    Returns None if the table is empty; MAX(id)+1 if every row is older."""
    lo = db.execute(f"SELECT MIN(id) FROM {table}").fetchone()[0]
    hi = db.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
    if lo is None:
        return None
    if str(db.execute(f"SELECT {ts_col} FROM {table} WHERE id=?", (hi,)).fetchone()[0]) < cutoff:
        return hi + 1

    def first_from(i: int) -> tuple[int, str]:
        row = db.execute(f"SELECT id,{ts_col} FROM {table} WHERE id>=? ORDER BY id LIMIT 1", (i,)).fetchone()
        return int(row[0]), str(row[1])

    while lo < hi:
        mid = (lo + hi) // 2
        if first_from(mid)[1] >= cutoff:
            hi = mid
        else:
            lo = mid + 1
    return first_from(lo)[0]


def _book_keep_plan(db: sqlite3.Connection, keep_hours: float,
                    sides: set[str] | None = None) -> tuple[str, int | None, dict[str, str]]:
    """Cutoff for price history, plus active tokens whose latest full snapshot is older
    than the cutoff (we must keep that snapshot and everything after it, because the
    current price is rebuilt from the last snapshot + later updates)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=keep_hours)).isoformat()
    cut_id = _first_id_at_or_after(db, "book_events", "received_at", cutoff)
    lookback = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()  # bounds the search
    protect: dict[str, str] = {}
    for token, side in db.execute(
            "SELECT t.token_id,t.side FROM tokens t JOIN markets m ON m.id=t.market_id WHERE m.active=1").fetchall():
        if sides and str(side).upper() not in sides:
            continue
        row = db.execute(
            """SELECT received_at FROM book_events WHERE token_id=? AND event_type='book'
                 AND received_at>=? ORDER BY received_at DESC LIMIT 1""", (token, lookback)).fetchone()
        if row and str(row[0]) < cutoff:
            protect[str(token)] = str(row[0])
    return cutoff, cut_id, protect


def _table_exists(db: sqlite3.Connection, name: str, schema: str = "main") -> bool:
    return db.execute(f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
                      (name,)).fetchone() is not None


def _traded_sides(cfg: dict) -> set[str]:
    return {str(s).upper() for s in cfg.get("decision", {}).get("allowed_sides", ("YES", "NO"))}


def prune_db(db_path: str, cfg: dict) -> dict:
    """Delete old price history and old decision detail in small batches (safe while
    live.py runs). Freed space is reused by SQLite, so the file stops growing."""
    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    out = {"book_events": 0, "live_decisions": 0, "live_model_diagnostics": 0, "live_decision_runs": 0}
    try:
        _, cut_id, protect = _book_keep_plan(db, float(cfg.get("keep_book_hours", 12)), _traded_sides(cfg))
        if cut_id is not None:
            db.execute("CREATE TEMP TABLE IF NOT EXISTS keep_tokens(token_id TEXT PRIMARY KEY)")
            db.execute("DELETE FROM temp.keep_tokens")
            db.executemany("INSERT INTO temp.keep_tokens VALUES (?)", [(t,) for t in protect])
            start = db.execute("SELECT MIN(id) FROM book_events").fetchone()[0] or cut_id
            while start < cut_id:
                end = min(start + 20000, cut_id)
                out["book_events"] += db.execute(
                    """DELETE FROM book_events WHERE id>=? AND id<?
                       AND (token_id IS NULL OR token_id NOT IN (SELECT token_id FROM temp.keep_tokens))""",
                    (start, end)).rowcount
                db.commit()
                start = end
            for token, snapshot_at in protect.items():
                out["book_events"] += db.execute(
                    "DELETE FROM book_events WHERE token_id=? AND received_at<?", (token, snapshot_at)).rowcount
                db.commit()
        if _table_exists(db, "live_decision_runs"):
            cutoff = (datetime.now(timezone.utc) - timedelta(days=float(cfg.get("keep_decision_days", 7)))).isoformat()
            run_cut = _first_id_at_or_after(db, "live_decision_runs", "as_of", cutoff)
            if run_cut is not None:
                for table in ("live_decisions", "live_model_diagnostics"):
                    if not _table_exists(db, table):
                        continue
                    while True:
                        n = db.execute(f"""DELETE FROM {table} WHERE rowid IN (
                            SELECT rowid FROM {table} WHERE run_id<? LIMIT 20000)""", (run_cut,)).rowcount
                        db.commit()
                        out[table] += n
                        if n == 0:
                            break
                keep_audit = (" AND id NOT IN (SELECT run_id FROM paper_candidate_audit)"
                              if _table_exists(db, "paper_candidate_audit") else "")
                out["live_decision_runs"] = db.execute(
                    f"DELETE FROM live_decision_runs WHERE id<?{keep_audit}", (run_cut,)).rowcount
                db.commit()
    finally:
        db.close()
    return out


def compact_db(db_path: Path, cfg: dict) -> None:
    """One-time rebuild into a small new file (run with collector.py and live.py STOPPED).

    Copies everything except old price history / old decision detail, then swaps files:
    the old database is kept as weather.old.sqlite3 until you delete it.
    """
    import shutil
    new_path = db_path.with_name(db_path.stem + ".compact" + db_path.suffix)
    old_backup = db_path.with_name(db_path.stem + ".old" + db_path.suffix)
    if old_backup.exists():
        raise SystemExit(f"{old_backup.name} already exists; delete it first (it is a previous copy).")
    free_gb = shutil.disk_usage(db_path.parent).free / 1e9
    print(f"Free disk space: {free_gb:.1f} GB")
    if free_gb < 15:
        raise SystemExit("Need at least ~15 GB free. Delete the old backup databases first.")
    keep_hours = float(cfg.get("keep_book_hours", 12))
    keep_days = float(cfg.get("keep_decision_days", 7))

    src = sqlite3.connect(db_path, timeout=5)
    busy, _, _ = src.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if busy:
        src.close()
        raise SystemExit("Database is in use. Stop collector.py and live.py first, then retry.")
    print("Finding what to keep ...", flush=True)
    _, cut_id, protect = _book_keep_plan(src, keep_hours, _traded_sides(cfg))
    run_cut = None
    if _table_exists(src, "live_decision_runs"):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
        run_cut = _first_id_at_or_after(src, "live_decision_runs", "as_of", cutoff)
    src.close()

    if new_path.exists():
        new_path.unlink()
    dst = sqlite3.connect(new_path)
    dst.execute("PRAGMA journal_mode=OFF")
    dst.execute("PRAGMA synchronous=OFF")
    dst.execute("ATTACH DATABASE ? AS o", (str(db_path),))
    tables = dst.execute("""SELECT name,sql FROM o.sqlite_master
                            WHERE type='table' AND name NOT LIKE 'sqlite_%'""").fetchall()
    for name, sql in tables:
        dst.execute(sql)
        started = time.monotonic()
        print(f"  copying {name} ...", end=" ", flush=True)
        if name == "book_events" and cut_id is not None:
            dst.execute("INSERT INTO main.book_events SELECT * FROM o.book_events WHERE id>=?", (cut_id,))
            for token, snapshot_at in protect.items():
                dst.execute("""INSERT INTO main.book_events SELECT * FROM o.book_events
                               WHERE token_id=? AND received_at>=? AND id<?""", (token, snapshot_at, cut_id))
        elif name in ("live_decisions", "live_model_diagnostics") and run_cut is not None:
            dst.execute(f'INSERT INTO main."{name}" SELECT * FROM o."{name}" WHERE run_id>=?', (run_cut,))
        else:
            dst.execute(f'INSERT INTO main."{name}" SELECT * FROM o."{name}"')
        dst.commit()
        rows = dst.execute(f'SELECT count(*) FROM main."{name}"').fetchone()[0]
        print(f"{rows:,} rows in {time.monotonic() - started:.0f}s", flush=True)
    print("  rebuilding indexes ...", flush=True)
    for (sql,) in dst.execute("SELECT sql FROM o.sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall():
        dst.execute(sql)
    dst.commit()
    dst.execute("DETACH DATABASE o")
    dst.execute("PRAGMA journal_mode=WAL")
    dst.close()

    for suffix in ("-wal", "-shm"):
        extra = Path(str(db_path) + suffix)
        if extra.exists():
            extra.unlink()
    db_path.rename(old_backup)
    new_path.rename(db_path)
    print(f"\nDone. New {db_path.name}: {db_path.stat().st_size / 1e9:.2f} GB")
    print(f"Old database kept as {old_backup.name} ({old_backup.stat().st_size / 1e9:.1f} GB).")
    print("Start collector.py + live.py, check 'python paper.py status', then delete the old file.")


def import_observations(db: sqlite3.Connection, path: str) -> None:
    with open(path, newline="", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))
    required = {"market_id", "station", "observed_at", "temperature", "unit", "source_url"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError("CSV needs headers: " + ", ".join(sorted(required)))
    with db:
        for row in rows:
            if not all(row.get(key, "").strip() for key in required):
                raise ValueError("Empty required observation field")
            timestamp = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("observed_at needs an explicit UTC offset")
            db.execute("INSERT INTO observations(market_id,station,observed_at,temperature,unit,source_url,source_record_id,fetched_at,raw_json) VALUES (?,?,?,?,?,?,?,?,?)",
                       (row["market_id"], row["station"], timestamp.astimezone(timezone.utc).isoformat(),
                        float(row["temperature"]), row["unit"], row["source_url"], row.get("source_record_id"), utc_now(), json.dumps(row)))
    print(f"Imported {len(rows)} observations")
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("collect", "status", "import-observations", "compact"))
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--event-id", action="append", help="Pin event in CLOB subscription (repeatable)")
    parser.add_argument("--csv")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    cfg["_config_path"] = str(config_path)
    if args.event_id and args.command == "collect":
        cfg["event_ids"] = list(dict.fromkeys([*cfg.get("event_ids", []), *args.event_id]))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
    cfg["_db_path"] = str(db_path)
    if args.command == "compact":
        compact_db(db_path, cfg)
        return
    db = db_open(str(db_path))
    try:
        if args.command == "status":
            for table in ("events", "markets", "tokens", "rule_reviews", "forecast_batches", "forecast_values", "book_events", "observations"):
                print(f"{table}: {db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]}")
            return
        if args.command == "import-observations":
            if not args.csv:
                parser.error("import-observations needs --csv")
            import_observations(db, args.csv)
            return
        asyncio.run(collect(db, cfg, args.seconds, args.once))
    except KeyboardInterrupt:
        LOG.info("Stopped")
    finally:
        db.close()
if __name__ == "__main__":
    main()
