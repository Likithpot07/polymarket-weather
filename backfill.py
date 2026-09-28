"""Centralized historical backfill for resolved Polymarket weather markets.
Sources used:
- Gamma API: resolved event discovery, market metadata, rules, token mapping, settlement
- Polymarket CLOB prices-history: historical token prices (not historical L2 depth)
- Open-Meteo Single Runs: archived ECMWF/GFS/ICON forecasts
The script never deletes live collector data. It only replaces rows previously created by this backfill source.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import csv
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import socket
import sqlite3
import time
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import truststore
from dotenv import load_dotenv
truststore.inject_into_ssl()
load_dotenv(Path(__file__).resolve().parent / ".env")
try:
    import dns.resolver
except ImportError:
    dns = None
from collector import DDL, candidate_event, save_event

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER = "https://single-runs-api.open-meteo.com/v1/forecast"
MARKETLENS = "https://api.marketlens.trade/v1"
MODELS = ("ecmwf_ifs025", "gfs_global", "icon_global")
TAG_ID = 104596
SOURCE = "central_backfill"
_APP_DNS_HOSTS = {"gamma-api.polymarket.com", "clob.polymarket.com"}
_ORIGINAL_GETADDRINFO = socket.getaddrinfo

def app_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host not in _APP_DNS_HOSTS or dns is None:
        return _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = ["1.1.1.1", "1.0.0.1"]
    resolver.lifetime = 6.0
    results = []
    for answer in resolver.resolve(host, "A"):
        results.extend(_ORIGINAL_GETADDRINFO(str(answer), port, socket.AF_INET, type, proto, flags))
    return results

socket.getaddrinfo = app_getaddrinfo

def fetch_json(url: str, timeout: int = 60, attempts: int = 3):
    error = None
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"Accept": "application/json", "User-Agent": "polymarket-weather-backfill/1"})
            with urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except Exception as exc:
            error = exc
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Request failed after {attempts} attempts: {url}\n{error}")

def fetch_marketlens_json(path: str, api_key: str, params: dict | None = None, timeout: int = 60):
    url = f"{MARKETLENS}{path}"
    if params:
        url += "?" + urlencode(params)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "polymarket-weather-backfill/1",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body[:1000]}
        return exc.code, payload
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return 0, {"error": str(exc)}

def marketlens_snapshot(condition_id: str, decision: datetime, api_key: str, depth: int) -> tuple[dict | None, str | None]:
    status, payload = fetch_marketlens_json(
        f"/markets/{quote(condition_id, safe='')}/orderbook",
        api_key,
        {
            "at": decision.isoformat().replace("+00:00", "Z"),
            "depth": depth,
            "nearest": "before",
        },
    )
    if status == 200 and isinstance(payload, dict):
        return payload, None
    if status in (404, 422):
        return None, "DATA_NOT_AVAILABLE"
    error_obj = payload.get("error") if isinstance(payload, dict) else None
    error_code = error_obj.get("code") if isinstance(error_obj, dict) else None
    if status == 403 and error_code == "HISTORY_WINDOW_EXCEEDED":
        return None, "HISTORY_WINDOW_EXCEEDED"
    if status in (401, 403):
        return None, f"AUTH_{status}"
    error = error_obj if error_obj is not None else payload
    return None, f"HTTP_{status}:{error}"

def _normalize_levels(levels) -> list[dict]:
    result = []
    for level in levels or []:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level["price"])
            size = float(level["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if 0 <= price <= 1 and size > 0 and math.isfinite(price) and math.isfinite(size):
            result.append({"price": price, "size": size})
    return result

def _no_side_book(yes_bids: list[dict], yes_asks: list[dict]) -> tuple[list[dict], list[dict]]:
    no_bids = [{"price": 1.0 - level["price"], "size": level["size"]} for level in yes_asks]
    no_asks = [{"price": 1.0 - level["price"], "size": level["size"]} for level in yes_bids]
    no_bids.sort(key=lambda x: x["price"], reverse=True)
    no_asks.sort(key=lambda x: x["price"])
    return no_bids, no_asks

def import_marketlens_books(
    db: sqlite3.Connection,
    event_id: str,
    decisions: list[datetime],
    api_key: str,
    depth: int,
    delay_seconds: float,
) -> dict:
    market_rows = db.execute(
        """SELECT m.id,m.condition_id,t.token_id,t.side
           FROM markets m
           JOIN tokens t ON t.market_id=m.id
           WHERE m.event_id=? AND m.condition_id IS NOT NULL
           ORDER BY m.id,t.side""",
        (event_id,),
    ).fetchall()
    by_market: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for market_id, condition_id, token_id, side in market_rows:
        by_market[(str(market_id), str(condition_id))][str(side).upper()] = str(token_id)

    event_tokens = [row[0] for row in db.execute(
        "SELECT t.token_id FROM tokens t JOIN markets m ON m.id=t.market_id WHERE m.event_id=?",
        (event_id,),
    )]
    for token_id in event_tokens:
        db.execute(
            """DELETE FROM book_events
               WHERE token_id=? AND event_type='book'
                 AND json_extract(raw_json,'$.source')='marketlens'""",
            (token_id,),
        )

    covered: set[tuple[str, str]] = set()
    snapshots = 0
    unavailable = 0
    errors = 0
    auth_failed = False

    for (_, condition_id), sides in by_market.items():
        for decision in decisions:
            if auth_failed:
                break
            book, error = marketlens_snapshot(condition_id, decision, api_key, depth)
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            if error:
                if error.startswith("AUTH_"):
                    auth_failed = True
                elif error in ("DATA_NOT_AVAILABLE", "HISTORY_WINDOW_EXCEEDED"):
                    unavailable += 1
                else:
                    errors += 1
                continue

            yes_bids = _normalize_levels(book.get("bids"))
            yes_asks = _normalize_levels(book.get("asks"))
            if not yes_bids or not yes_asks:
                unavailable += 1
                continue

            as_of_value = book.get("as_of")
            try:
                as_of = (
                    datetime.fromtimestamp(float(as_of_value) / 1000.0, timezone.utc)
                    if isinstance(as_of_value, (int, float)) or str(as_of_value).isdigit()
                    else parse_utc(as_of_value)
                )
            except (TypeError, ValueError, OSError):
                as_of = decision

            side_books = {"YES": (yes_bids, yes_asks)}
            side_books["NO"] = _no_side_book(yes_bids, yes_asks)

            for side, (bids, asks) in side_books.items():
                token_id = sides.get(side)
                if not token_id or not bids or not asks:
                    continue
                best_bid = max(level["price"] for level in bids)
                best_ask = min(level["price"] for level in asks)
                if not (0 <= best_bid < best_ask <= 1):
                    continue
                raw = {
                    "source": "marketlens",
                    "kind": "historical_l2",
                    "platform": "polymarket",
                    "condition_id": condition_id,
                    "side": side,
                    "requested_at": decision.isoformat(),
                    "as_of": as_of.isoformat(),
                    "nearest": "before",
                    "depth_requested": depth,
                    "bids": bids,
                    "asks": asks,
                    "marketlens_summary": {
                        "spread": book.get("spread"),
                        "midpoint": book.get("midpoint"),
                        "bid_depth": book.get("bid_depth"),
                        "ask_depth": book.get("ask_depth"),
                        "bid_levels": book.get("bid_levels"),
                        "ask_levels": book.get("ask_levels"),
                    },
                }
                db.execute(
                    """INSERT INTO book_events(
                           token_id,event_type,source_timestamp,received_at,
                           best_bid,best_ask,raw_json
                       ) VALUES (?,?,?,?,?,?,?)""",
                    (
                        token_id,
                        "book",
                        as_of.isoformat(),
                        as_of.isoformat(),
                        str(best_bid),
                        str(best_ask),
                        json.dumps(raw, separators=(",", ":")),
                    ),
                )
                covered.add((token_id, decision.isoformat()))
                snapshots += 1

        if auth_failed:
            break

    return {
        "covered": covered,
        "snapshots": snapshots,
        "unavailable": unavailable,
        "errors": errors,
        "auth_failed": auth_failed,
    }

def as_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []

def parse_utc(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)

def target_date(event: dict) -> str | None:
    text = f"{event.get('title', '')} {event.get('slug', '')}"
    iso = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if iso:
        candidate = f"{iso.group(1)}-{iso.group(2)}-{iso.group(3)}"
        try:
            return date.fromisoformat(candidate).isoformat()
        except ValueError:
            return None
    months = "January|February|March|April|May|June|July|August|September|October|November|December"
    match = re.search(rf"\b({months})\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(20\d{{2}}))?\b", text, re.IGNORECASE)
    if not match:
        return None
    year = int(match.group(3) or datetime.now().year)
    try:
        return datetime.strptime(f"{match.group(1)} {match.group(2)} {year}", "%B %d %Y").date().isoformat()
    except ValueError:
        return None

def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(DDL)
    return db

def get_event(event_id: str) -> dict:
    event = fetch_json(f"{GAMMA}/events/{quote(str(event_id))}")
    if not isinstance(event, dict) or str(event.get("id")) != str(event_id):
        raise ValueError(f"Gamma did not return event {event_id}")
    return event

def discover_events(start: date, end: date, max_pages: int = 20) -> list[dict]:
    api_start = datetime.combine(start - timedelta(days=2), datetime.min.time(), timezone.utc)
    api_end = datetime.combine(end + timedelta(days=2), datetime.max.time(), timezone.utc)
    cursor = None
    seen_cursors = set()
    scanned: dict[str, dict] = {}
    for page_number in range(1, max_pages + 1):
        params = {
            "tag_id": TAG_ID,
            "closed": "true",
            "limit": 100,
            "order": "endDate",
            "ascending": "true",
            "end_date_min": api_start.isoformat(),
            "end_date_max": api_end.isoformat(),
        }
        if cursor:
            params["after_cursor"] = cursor
        print(f"Discovery page {page_number}...", flush=True)
        payload = fetch_json(f"{GAMMA}/events/keyset?{urlencode(params)}")
        page = payload.get("events", []) if isinstance(payload, dict) else []
        print(f"  received {len(page)} events", flush=True)
        for event in page:
            scanned[str(event["id"])] = event
        next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
        if not page or not next_cursor or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    selected = []
    for event in scanned.values():
        event_date = target_date(event)
        if not candidate_event(event) or not event_date:
            continue
        if start <= date.fromisoformat(event_date) <= end:
            selected.append(event)
    selected.sort(key=lambda e: (target_date(e) or "", str(e["id"])))
    return selected

def balanced_sample(events: list[dict], per_date: int) -> list[dict]:
    """Deterministically rotate selections across cities instead of always taking alphabetic extremes."""
    if per_date <= 0:
        return events
    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        key = target_date(event)
        if key:
            grouped[key].append(event)
    selected = []
    for key in sorted(grouped):
        candidates = sorted(
            grouped[key],
            key=lambda e: (str(e.get("title") or ""), str(e.get("id") or "")),
        )
        if len(candidates) <= per_date:
            chosen = candidates
        else:
            n = len(candidates)
            offset = date.fromisoformat(key).toordinal() % n
            chosen_indexes = []
            for i in range(per_date):
                index = (offset + (i * n // per_date)) % n
                while index in chosen_indexes:
                    index = (index + 1) % n
                chosen_indexes.append(index)
            chosen = [candidates[index] for index in chosen_indexes]
        selected.extend(chosen)
    return selected

def ensure_location(db: sqlite3.Connection, city: str) -> tuple[float, float, str]:
    row = db.execute("SELECT latitude,longitude,timezone FROM locations WHERE city=?", (city,)).fetchone()
    if row:
        return float(row[0]), float(row[1]), str(row[2])
    payload = fetch_json(f"{GEOCODE}?{urlencode({'name': city, 'count': 1, 'language': 'en'})}")
    results = payload.get("results") or []
    if not results:
        raise ValueError(f"No Open-Meteo location found for {city}")
    item = results[0]
    latitude = float(item["latitude"])
    longitude = float(item["longitude"])
    timezone_name = str(item["timezone"])
    db.execute(
        "INSERT INTO locations VALUES (?,?,?,?,?,?) ON CONFLICT(city) DO UPDATE SET "
        "latitude=excluded.latitude,longitude=excluded.longitude,timezone=excluded.timezone,"
        "source=excluded.source,raw_json=excluded.raw_json",
        (city, latitude, longitude, timezone_name, "open-meteo-geocoding", json.dumps(item)),
    )
    return latitude, longitude, timezone_name

def forecast_run(target: str, timezone_name: str, lead_hours: int) -> datetime:
    local_start = datetime.combine(date.fromisoformat(target), datetime.min.time(), ZoneInfo(timezone_name))
    wanted = local_start.astimezone(timezone.utc) - timedelta(hours=lead_hours)
    return wanted.replace(hour=(wanted.hour // 6) * 6, minute=0, second=0, microsecond=0)

def import_forecasts(
    db: sqlite3.Connection,
    city: str,
    target: str,
    latitude: float,
    longitude: float,
    timezone_name: str,
    lead_hours: list[int],
    lag_hours: float,
) -> list[datetime]:
    decisions: list[datetime] = []
    for lead in lead_hours:
        run = forecast_run(target, timezone_name, lead)
        decision = run + timedelta(hours=lag_hours)
        marker = f"{SOURCE}:{city}:{target}:{run.isoformat()}:"
        old = [row[0] for row in db.execute("SELECT id FROM forecast_batches WHERE endpoint LIKE ?", (marker + "%",))]
        for batch_id in old:
            db.execute("DELETE FROM forecast_values WHERE batch_id=?", (batch_id,))
            db.execute("DELETE FROM forecast_batches WHERE id=?", (batch_id,))
        for model in MODELS:
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
            url = f"{WEATHER}?{params}"
            payload = fetch_json(url)
            hourly = payload.get("hourly") or {}
            pairs = [
                (stamp, float(value))
                for stamp, value in zip(hourly.get("time", []), hourly.get("temperature_2m", []))
                if value is not None
            ]
            target_values = [value for stamp, value in pairs if stamp.startswith(target)]
            if not target_values:
                raise ValueError(f"{model} run {run.isoformat()} has no {target} forecast")
            cursor = db.execute(
                "INSERT INTO forecast_batches(city,model,fetched_at,model_run_at,endpoint,raw_json) VALUES (?,?,?,?,?,?)",
                (city, model, decision.isoformat(), run.isoformat(), marker + model, json.dumps(payload)),
            )
            local_zone = ZoneInfo(timezone_name)
            for stamp, value in pairs:
                valid = datetime.fromisoformat(stamp).replace(tzinfo=local_zone).isoformat()
                db.execute("INSERT INTO forecast_values VALUES (?,?,?,?)", (cursor.lastrowid, "hourly", valid, value))
            db.execute("INSERT INTO forecast_values VALUES (?,?,?,?)", (cursor.lastrowid, "daily_max", target, max(target_values)))
        decisions.append(decision)
    return decisions

def price_history(token_id: str, start: datetime, end: datetime, fidelity: int) -> list[tuple[datetime, float]]:
    params = urlencode({
        "market": token_id,
        "startTs": int(start.timestamp()),
        "endTs": int(end.timestamp()),
        "fidelity": fidelity,
    })
    payload = fetch_json(f"{CLOB}/prices-history?{params}")
    rows = payload.get("history", []) if isinstance(payload, dict) else []
    result = []
    for row in rows:
        try:
            stamp = parse_utc(row["t"])
            price = float(row["p"])
            if 0 < price < 1 and math.isfinite(price):
                result.append((stamp, price))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return sorted(result)

def import_price_proxies(
    db: sqlite3.Connection,
    event_id: str,
    decisions: list[datetime],
    fidelity: int,
    half_spread: float,
    covered: set[tuple[str, str]] | None = None,
) -> int:
    if not decisions:
        return 0
    covered = covered or set()
    token_rows = db.execute(
        "SELECT t.token_id FROM tokens t JOIN markets m ON m.id=t.market_id WHERE m.event_id=?",
        (event_id,),
    ).fetchall()
    inserted = 0
    for (token_id,) in token_rows:
        start = min(decisions) - timedelta(days=2)
        end = max(decisions) + timedelta(hours=1)
        history = price_history(str(token_id), start, end, fidelity)
        db.execute(
            """DELETE FROM book_events
               WHERE token_id=?
                 AND event_type IN ('historical_price_proxy','best_bid_ask')
                 AND json_extract(raw_json,'$.source')=?""",
            (token_id, SOURCE),
        )
        for decision in decisions:
            if (str(token_id), decision.isoformat()) in covered:
                continue
            available = [(stamp, price) for stamp, price in history if stamp <= decision]
            if not available:
                continue
            observed_at, price = available[-1]
            bid = max(0.001, price - half_spread)
            ask = min(0.999, price + half_spread)
            if bid >= ask:
                continue
            raw = {
                "source": SOURCE,
                "kind": "historical_price_proxy",
                "price": price,
                "observed_at": observed_at.isoformat(),
                "assumed_half_spread": half_spread,
                "warning": "Not a recorded bid/ask or full order book",
            }
            db.execute(
                "INSERT INTO book_events(token_id,event_type,source_timestamp,received_at,best_bid,best_ask,raw_json) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    token_id,
                    "best_bid_ask",
                    observed_at.isoformat(),
                    observed_at.isoformat(),
                    str(bid),
                    str(ask),
                    json.dumps(raw, separators=(",", ":")),
                ),
            )
            inserted += 1
    return inserted

def settled_labels(event: dict, verified_at: str) -> list[dict]:
    labels = []
    for market in event.get("markets") or []:
        outcomes = as_list(market.get("outcomes"))
        prices = as_list(market.get("outcomePrices"))
        try:
            numeric = [float(value) for value in prices]
        except (TypeError, ValueError):
            continue
        if [str(x).lower() for x in outcomes] != ["yes", "no"] or not numeric:
            continue
        if max(numeric) < 0.99:
            continue
        labels.append({
            "event_id": str(event["id"]),
            "market_id": str(market["id"]),
            "yes_result": 1 if numeric[0] >= 0.99 else 0,
            "source_url": f"{GAMMA}/markets/{market['id']}",
            "verified_at": verified_at,
        })
    return labels

def write_labels(path: Path, new_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["event_id", "market_id", "yes_result", "source_url", "verified_at"]
    existing = []
    if path.exists():
        with path.open(newline="", encoding="utf-8-sig") as handle:
            existing = list(csv.DictReader(handle))
    merged = {(row["event_id"], row["market_id"]): row for row in existing}
    merged.update({(row["event_id"], row["market_id"]): row for row in new_rows})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(merged.values(), key=lambda row: (row["event_id"], row["market_id"])))

def write_failures(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=("event_id", "target_date", "error"))
        writer.writeheader()
        writer.writerows(rows)

def import_event(db: sqlite3.Connection, event: dict, args) -> dict:
    if not candidate_event(event):
        raise ValueError("not a highest-temperature event")
    if not event.get("closed"):
        raise ValueError("event is not closed")
    event_id = str(event["id"])
    save_event(db, event)
    row = db.execute("SELECT city,target_date FROM markets WHERE event_id=? LIMIT 1", (event_id,)).fetchone()
    if not row or not all(row):
        raise ValueError("city or target date could not be parsed")
    city, target = str(row[0]), str(row[1])
    latitude, longitude, timezone_name = ensure_location(db, city)
    decisions = import_forecasts(
        db,
        city,
        target,
        latitude,
        longitude,
        timezone_name,
        args.lead_hours,
        args.availability_lag_hours,
    )
    marketlens = {
        "covered": set(),
        "snapshots": 0,
        "unavailable": 0,
        "errors": 0,
        "auth_failed": False,
    }
    if args.marketlens_api_key:
        marketlens = import_marketlens_books(
            db,
            event_id,
            decisions,
            args.marketlens_api_key,
            args.marketlens_depth,
            args.marketlens_delay_seconds,
        )
    if args.require_marketlens and marketlens["auth_failed"]:
        raise RuntimeError("MarketLens authentication failed")
    proxies = import_price_proxies(
        db,
        event_id,
        decisions,
        args.price_fidelity_minutes,
        args.assumed_half_spread,
        marketlens["covered"],
    )
    reviewed_at = datetime.now(timezone.utc).isoformat()
    if args.verify_rules:
        db.execute(
            "UPDATE rule_reviews SET verified=1,reviewed_at=?,"
            "notes=COALESCE(notes,'') || '; manually verified during historical backfill' "
            "WHERE market_id IN (SELECT id FROM markets WHERE event_id=?)",
            (reviewed_at, event_id),
        )
    labels = settled_labels(event, reviewed_at)
    return {
        "event_id": event_id,
        "city": city,
        "target_date": target,
        "forecast_cycles": len(decisions),
        "marketlens_l2_books": marketlens["snapshots"],
        "marketlens_unavailable": marketlens["unavailable"],
        "marketlens_errors": marketlens["errors"],
        "marketlens_auth_failed": marketlens["auth_failed"],
        "price_proxies": proxies,
        "labels": labels,
        "rules_verified": bool(args.verify_rules),
    }

def parse_lead_hours(value: str) -> list[int]:
    try:
        result = sorted({int(x.strip()) for x in value.split(",") if x.strip()}, reverse=True)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not result or any(x <= 0 for x in result):
        raise argparse.ArgumentTypeError("must contain positive integers")
    return result

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/weather.sqlite3")
    parser.add_argument("--event-id", action="append", default=[], help="Resolved Gamma event ID; repeatable")
    parser.add_argument("--start-date", help="Discover resolved events from YYYY-MM-DD")
    parser.add_argument("--end-date", help="Discovery end date; defaults to today")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-events", type=int, default=500)
    parser.add_argument("--events-per-date", type=int, default=0, help="0 keeps all discovered events; positive value takes a balanced sample per date")
    parser.add_argument("--lead-hours", type=parse_lead_hours, default=parse_lead_hours("36,24,12"))
    parser.add_argument("--availability-lag-hours", type=float, default=6.0)
    parser.add_argument("--price-fidelity-minutes", type=int, default=60)
    parser.add_argument("--assumed-half-spread", type=float, default=0.01)
    parser.add_argument("--marketlens-depth", type=int, default=20)
    parser.add_argument("--marketlens-delay-seconds", type=float, default=0.12)
    parser.add_argument("--no-marketlens", action="store_true")
    parser.add_argument("--require-marketlens", action="store_true")
    parser.add_argument("--labels-out", default="data/backfill/candidate_outcomes.csv")
    parser.add_argument("--failures-out", default="data/backfill/backfill_failures.csv")
    parser.add_argument("--verify-rules", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--discover-only", action="store_true")
    args = parser.parse_args()
    if not args.event_id and not args.start_date:
        parser.error("provide --event-id or --start-date")
    if args.max_pages <= 0 or args.max_events <= 0 or args.price_fidelity_minutes <= 0:
        parser.error("--max-pages, --max-events and --price-fidelity-minutes must be positive")
    if args.events_per_date < 0:
        parser.error("--events-per-date cannot be negative")
    if not 0 < args.assumed_half_spread < 0.5:
        parser.error("--assumed-half-spread must be between 0 and 0.5")
    if not 1 <= args.marketlens_depth <= 100:
        parser.error("--marketlens-depth must be between 1 and 100")
    if args.marketlens_delay_seconds < 0:
        parser.error("--marketlens-delay-seconds cannot be negative")
    args.marketlens_api_key = "" if args.no_marketlens else os.getenv("MARKETLENS_API_KEY", "").strip()
    if args.require_marketlens and not args.marketlens_api_key:
        parser.error("MARKETLENS_API_KEY is not set")
    print(
        f"MarketLens L2: {'enabled' if args.marketlens_api_key else 'disabled (price-proxy fallback only)'}",
        flush=True,
    )
    direct_events = [get_event(event_id) for event_id in dict.fromkeys(args.event_id)]
    discovered = []
    if args.start_date:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
        if start > end:
            parser.error("--start-date cannot be after --end-date")
        discovered = discover_events(start, end, args.max_pages)
    events = list({str(event["id"]): event for event in [*direct_events, *discovered]}.values())
    events.sort(key=lambda e: (target_date(e) or "", str(e["id"])))
    if args.events_per_date:
        events = balanced_sample(events, args.events_per_date)
    events = events[:args.max_events]
    print(f"Resolved weather events selected: {len(events)}", flush=True)
    for event in events:
        print(f"{event['id']} | {target_date(event)} | {event.get('title')}", flush=True)
    if args.discover_only:
        print(json.dumps({"status": "DISCOVERY_COMPLETE", "events": len(events), "database_modified": False}, indent=2))
        return
    hydrated = []
    direct_by_id = {str(event["id"]): event for event in direct_events}
    for number, event in enumerate(events, 1):
        event_id = str(event["id"])
        if event_id in direct_by_id:
            hydrated.append(direct_by_id[event_id])
            continue
        print(f"Loading selected event {number}/{len(events)}: {event_id}", flush=True)
        hydrated.append(get_event(event_id))
    events = hydrated
    db = init_db(Path(args.db))
    summaries = []
    all_labels = []
    failures = []
    try:
        for number, event in enumerate(events, 1):
            event_id = str(event["id"])
            event_date = target_date(event) or ""
            print(f"[{number}/{len(events)}] {event_id} | {event_date} | {event.get('title')}", flush=True)
            try:
                with db:
                    summary = import_event(db, event, args)
                summaries.append({k: v for k, v in summary.items() if k != "labels"})
                all_labels.extend(summary["labels"])
                write_labels(Path(args.labels_out), all_labels)
                print(
                    f"  imported: {summary['forecast_cycles']} forecast cycles, "
                    f"{summary['marketlens_l2_books']} MarketLens L2 token-books, "
                    f"{summary['price_proxies']} price proxies "
                    f"(L2 unavailable={summary['marketlens_unavailable']}, errors={summary['marketlens_errors']})",
                    flush=True,
                )
                if summary["marketlens_auth_failed"] and not args.require_marketlens:
                    print("  MarketLens auth failed; disabling MarketLens for remaining events.", flush=True)
                    args.marketlens_api_key = ""
            except Exception as exc:
                db.rollback()
                failures.append({"event_id": event_id, "target_date": event_date, "error": str(exc)})
                print(f"  skipped: {exc}", flush=True)
                if not args.continue_on_error:
                    raise
    finally:
        db.close()
    write_labels(Path(args.labels_out), all_labels)
    write_failures(Path(args.failures_out), failures)
    print(json.dumps({
        "status": "COMPLETE",
        "events_selected": len(events),
        "events_imported": len(summaries),
        "events_failed": len(failures),
        "candidate_labels": len(all_labels),
        "labels_file": str(Path(args.labels_out)),
        "failures_file": str(Path(args.failures_out)),
        "marketlens_enabled": bool(args.marketlens_api_key),
        "marketlens_depth": args.marketlens_depth if args.marketlens_api_key else None,
        "warning": "MarketLens L2 is used when available; CLOB historical price + assumed spread is used only as fallback.",
        "database_modified": True,
    }, indent=2))

if __name__ == "__main__":
    main()
