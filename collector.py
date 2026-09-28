"""Read-only weather market recorder. Python 3.11+."""
from __future__ import annotations
import argparse
import asyncio
import csv
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sqlite3
import time
import socket
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
    db = sqlite3.connect(target)
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
    if re.search(r"degrees?\s+Fahrenheit|°F", text, re.I):
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
    if pinned_count > cfg["max_tokens"]:
        raise ValueError(f"Pinned events require {pinned_count} tokens, exceeding max_tokens={cfg['max_tokens']}")
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
    selected = dict(list(token_map.items())[:cfg["max_tokens"]])
    LOG.info("CLOB tokens selected: %d/%d (pinned: %d)", len(selected), len(token_map), pinned_count)
    return selected
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
        for model in cfg["weather_models"]:
            params = dict(latitude=loc["latitude"], longitude=loc["longitude"],
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
async def stream_books(db: sqlite3.Connection, tokens: set[str]) -> None:
    import websockets
    delay = 1
    while tokens:
        try:
            async with websockets.connect(WS, ping_interval=None, max_size=8_000_000) as ws:
                await ws.send(json.dumps({"assets_ids": sorted(tokens), "type": "market", "custom_feature_enabled": True}))
                LOG.info("CLOB connected | %d tokens", len(tokens))
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
            LOG.warning("CLOB disconnected (%s); retrying in %ss", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
async def collect(
    db: sqlite3.Connection,
    cfg: dict,
    seconds: float | None,
    once: bool,
) -> None:
    ws_task = None
    current: set[str] = set()
    last_forecast = 0.0
    last_observation = 0.0
    try:
        while True:
            # 1. Discover active Polymarket weather markets/tokens.
            token_map = await scan(db, cfg)
            tokens = set(token_map)
            if not tokens:
                LOG.warning(
                    "No tokens found; check Gamma connectivity or add event_slugs"
                )
            # Snapshot mode: metadata + observations + forecasts, then exit.
            if once:
                await collect_observations(db, cfg)
                await collect_forecasts(db, cfg)
                return
            # 2. Start/refresh CLOB websocket immediately.
            if tokens != current or ws_task is None or ws_task.done():
                if ws_task:
                    ws_task.cancel()
                    await asyncio.gather(
                        ws_task,
                        return_exceptions=True,
                    )
                current = tokens
                if current:
                    ws_task = asyncio.create_task(
                        stream_books(db, current)
                    )
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
            # Short test mode. Runtime starts after initial refreshes.
            if seconds is not None:
                LOG.info(
                    "Collector running for %.0f seconds",
                    seconds,
                )
                await asyncio.sleep(seconds)
                return
            # 5. Normal continuous mode.
            await asyncio.sleep(
                cfg["scan_interval_seconds"]
            )
    finally:
        if ws_task:
            ws_task.cancel()
            await asyncio.gather(
                ws_task,
                return_exceptions=True,
            )
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
    parser.add_argument("command", choices=("collect", "status", "import-observations"))
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--event-id", action="append", help="Pin event in CLOB subscription (repeatable)")
    parser.add_argument("--csv")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    if args.event_id and args.command == "collect":
        cfg["event_ids"] = list(dict.fromkeys([*cfg.get("event_ids", []), *args.event_id]))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
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
