"""Strictly verify stored weather-market resolution rules against live Polymarket rules."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3

from collector import DDL, GAMMA, candidate_event, extract_date, get_json, parse_rule_metadata, save_event
from probability import bracket, event_markets

REQUIRED_RULE_PATTERNS = {
    "highest_temp": r"highest\s+temperature\s+recorded\s+by\s+NOAA",
    "temp_column": r'highest\s+reading\s+under\s+the\s+["“”]?Temp["“”]?\s+column',
    "hourly_data": r"Hourly\s+Data",
    "whole_degrees": r"whole\s+degrees?\s+(?:Fahrenheit|Celsius)",
    "fallback": r"Weather\s+Underground\s+Daily\s+Observations",
    "next_day_cutoff": r"first\s+data\s*point\s+for\s+the\s+following\s+date|first\s+datapoint\s+for\s+the\s+following\s+date",
}


def verify_event_payload(event: dict) -> dict:
    """Return normalized rule metadata or raise if the live rules are not strict enough to trust."""
    if not candidate_event(event):
        raise ValueError("event is not a highest-temperature market")
    target = extract_date(str(event.get("title") or ""), str(event.get("slug") or ""))
    if not target:
        raise ValueError("target date could not be parsed from event")
    markets = event.get("markets") or []
    if not markets:
        raise ValueError("event has no markets")

    normalized = []
    parsed_brackets = []
    for market in markets:
        description = str(market.get("description") or event.get("description") or "")
        resolution_source = str(
            market.get("resolutionSource")
            or event.get("resolutionSource")
            or ""
        )
        metadata = parse_rule_metadata(description, resolution_source, target)
        market_id = str(market.get("id") or "")
        if not market_id:
            raise ValueError("market missing id")
        if metadata["station"] is None:
            raise ValueError(f"market {market_id}: station not found in resolution source")
        if metadata["unit"] not in ("C", "F"):
            raise ValueError(f"market {market_id}: Celsius/Fahrenheit unit not confirmed")
        if metadata["rounding"] != "whole degree":
            raise ValueError(f"market {market_id}: whole-degree precision not confirmed")
        source = str(metadata["source_url"] or "")
        if not re.match(r"^https?://(?:www\.)?weather\.gov/wrh/timeseries\?", source, re.I):
            raise ValueError(f"market {market_id}: primary source is not NOAA weather.gov timeseries")
        if not re.search(r"[?&]site=" + re.escape(str(metadata["station"])) + r"\b", source, re.I):
            raise ValueError(f"market {market_id}: station/source mismatch")

        for name, pattern in REQUIRED_RULE_PATTERNS.items():
            if not re.search(pattern, description, re.I):
                raise ValueError(f"market {market_id}: required rule clause missing: {name}")

        parsed = bracket(str(market.get("question") or ""))
        if parsed is None:
            raise ValueError(f"market {market_id}: temperature bracket could not be parsed")
        parsed_brackets.append((market_id, *parsed))
        normalized.append({
            "market_id": market_id,
            "station": metadata["station"],
            "source_url": source,
            "unit": metadata["unit"],
            "rounding": metadata["rounding"],
            "time_window": metadata["time_window"],
        })

    signatures = {
        (r["station"], r["source_url"].lower(), r["unit"], r["rounding"], r["time_window"])
        for r in normalized
    }
    if len(signatures) != 1:
        raise ValueError("markets do not share one identical resolution rule set")

    units = {row[3] for row in parsed_brackets}
    if len(units) != 1 or next(iter(units)) != normalized[0]["unit"]:
        raise ValueError("market bracket units do not match the resolution rules")

    bins = sorted(parsed_brackets, key=lambda row: float("-inf") if row[1] is None else row[1])
    if bins[0][1] is not None or bins[-1][2] is not None:
        raise ValueError("event is missing a lower or upper tail bracket")
    if any(
        left[2] is None or right[1] is None or left[2] + 1 != right[1]
        for left, right in zip(bins, bins[1:])
    ):
        raise ValueError("temperature brackets overlap or contain gaps")

    rule = normalized[0]
    return {
        "event_id": str(event["id"]),
        "target_date": target,
        "market_count": len(normalized),
        "station": rule["station"],
        "source_url": rule["source_url"],
        "unit": rule["unit"],
        "rounding": rule["rounding"],
        "time_window": rule["time_window"],
        "checks": sorted(REQUIRED_RULE_PATTERNS),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--event-id", required=True)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config_path.parent / db_path
    if not db_path.is_file():
        parser.error(f"Collector database does not exist: {db_path}")

    event = get_json(f"{GAMMA}/events/{args.event_id}")
    if not isinstance(event, dict) or str(event.get("id")) != str(args.event_id):
        parser.error(f"Gamma did not return event {args.event_id}")
    try:
        verified = verify_event_payload(event)
    except ValueError as exc:
        parser.error(str(exc))

    reviewed_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path, timeout=30) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        db.executescript(DDL)
        with db:
            save_event(db, event)
            bins, city, target, unit, station, _ = event_markets(db, args.event_id)
            if station != verified["station"] or unit != verified["unit"] or target != verified["target_date"]:
                raise ValueError("stored event metadata does not match the verified live rules")
            db.execute(
                """UPDATE rule_reviews
                   SET verified=1,
                       station=?,
                       source_url=?,
                       unit=?,
                       rounding=?,
                       time_window=?,
                       reviewed_at=?,
                       notes=?
                   WHERE market_id IN (
                       SELECT id FROM markets WHERE event_id=?
                   )""",
                (
                    verified["station"],
                    verified["source_url"],
                    verified["unit"],
                    verified["rounding"],
                    verified["time_window"],
                    reviewed_at,
                    f"Strictly verified against live Polymarket rules: NOAA primary source, "
                    f"station/source match, Hourly Data Temp column, whole-degree {verified['unit']}, "
                    f"Weather Underground fallback, next-day publication cutoff, contiguous brackets.",
                    args.event_id,
                ),
            )
            count = db.execute(
                """SELECT count(*) FROM rule_reviews
                   WHERE verified=1
                     AND market_id IN (SELECT id FROM markets WHERE event_id=?)""",
                (args.event_id,),
            ).fetchone()[0]

    verified["verified_markets"] = count
    verified["reviewed_at"] = reviewed_at
    verified["status"] = "VERIFIED"
    print(json.dumps(verified, indent=2))


if __name__ == "__main__":
    main()
