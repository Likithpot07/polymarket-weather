"""Automatically verify rules and calibrate newly discovered active weather events."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo

from collector import DDL
from probability import DEFAULT_SIGMA_FLOOR_C, calibration, event_markets

PREP_DDL = """
CREATE TABLE IF NOT EXISTS event_preparation (
    event_id TEXT PRIMARY KEY,
    rules_status TEXT,
    calibration_status TEXT,
    rules_attempted_at TEXT,
    calibration_attempted_at TEXT,
    rules_error TEXT,
    calibration_error TEXT,
    updated_at TEXT NOT NULL
);
"""

MODELS = {"ecmwf_ifs025", "gfs_global", "icon_global"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_optional(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def retry_due(last_attempt: str | None, hours: float, now: datetime) -> bool:
    previous = parse_optional(last_attempt)
    return previous is None or (now - previous.astimezone(timezone.utc)).total_seconds() >= hours * 3600


def active_events(db: sqlite3.Connection, now: datetime) -> list[str]:
    rows = db.execute(
        """SELECT DISTINCT m.event_id,m.target_date,l.timezone
           FROM markets m
           LEFT JOIN locations l ON l.city=m.city
           WHERE m.active=1
             AND m.target_date IS NOT NULL
           ORDER BY m.target_date,m.event_id"""
    ).fetchall()
    result = []
    for event_id, target, timezone_name in rows:
        if not timezone_name:
            continue
        try:
            local_day = now.astimezone(ZoneInfo(str(timezone_name))).date()
            if local_day <= date.fromisoformat(str(target)):
                result.append(str(event_id))
        except (ValueError, TypeError):
            continue
    return result


def rules_ready(db: sqlite3.Connection, event_id: str) -> bool:
    total, verified = db.execute(
        """SELECT count(*),sum(CASE WHEN r.verified=1 THEN 1 ELSE 0 END)
           FROM markets m
           JOIN rule_reviews r ON r.market_id=m.id
           WHERE m.event_id=?""",
        (event_id,),
    ).fetchone()
    return bool(total and verified == total)


def calibration_ready(db: sqlite3.Connection, event_id: str, days: int, now: datetime) -> bool:
    try:
        bins, city, target, unit, station, _ = event_markets(db, event_id)
        if not station:
            return False
        row = db.execute("SELECT timezone FROM locations WHERE city=?", (city,)).fetchone()
        if not row:
            return False
        tz = ZoneInfo(str(row[0]))
        sigma_floor = DEFAULT_SIGMA_FLOOR_C if unit == "C" else DEFAULT_SIGMA_FLOOR_C * 9 / 5
        fitted = calibration(db, city, station, unit, target, now, tz, days, sigma_floor)
        return (
            MODELS.issubset(fitted)
            and all(int(fitted[model].get("days", 0)) >= days for model in MODELS)
        )
    except (ValueError, sqlite3.Error):
        return False


def run_script(script: Path, config: Path, event_id: str, extra: list[str] | None = None) -> tuple[bool, str]:
    command = [
        sys.executable,
        str(script),
        "--config",
        str(config),
        "--event-id",
        event_id,
    ]
    if extra:
        command.extend(extra)
    process = subprocess.run(
        command,
        cwd=script.parent,
        capture_output=True,
        text=True,
    )
    output = (process.stdout + "\n" + process.stderr).strip()
    if process.returncode == 0:
        return True, output
    tail = "\n".join(output.splitlines()[-8:])
    return False, tail or f"exit code {process.returncode}"


def update_state(db: sqlite3.Connection, event_id: str, **fields) -> None:
    allowed = {
        "rules_status",
        "calibration_status",
        "rules_attempted_at",
        "calibration_attempted_at",
        "rules_error",
        "calibration_error",
    }
    fields = {key: value for key, value in fields.items() if key in allowed}
    now_text = now_utc().isoformat()
    db.execute(
        """INSERT INTO event_preparation(event_id,updated_at)
           VALUES (?,?)
           ON CONFLICT(event_id) DO UPDATE SET updated_at=excluded.updated_at""",
        (event_id, now_text),
    )
    if fields:
        assignments = ",".join(f"{key}=?" for key in fields)
        db.execute(
            f"UPDATE event_preparation SET {assignments},updated_at=? WHERE event_id=?",
            (*fields.values(), now_text, event_id),
        )
    db.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    config = Path(args.config).resolve()
    cfg = json.loads(config.read_text(encoding="utf-8"))
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = config.parent / db_path
    days = int(cfg.get("calibration_days", 14))
    retry_hours = float(cfg.get("prepare_retry_hours", 6))
    now = now_utc()
    verify_script = Path(__file__).resolve().with_name("verify_rules.py")
    calibrate_script = Path(__file__).resolve().with_name("calibrate.py")

    with sqlite3.connect(db_path, timeout=30) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        db.executescript(DDL)
        db.executescript(PREP_DDL)
        events = active_events(db, now)
        print(f"AUTO PREP | active events={len(events)}", flush=True)

        for index, event_id in enumerate(events, 1):
            row = db.execute(
                """SELECT rules_attempted_at,calibration_attempted_at
                   FROM event_preparation WHERE event_id=?""",
                (event_id,),
            ).fetchone()
            rules_attempted = row[0] if row else None
            calibration_attempted = row[1] if row else None

            if rules_ready(db, event_id):
                update_state(db, event_id, rules_status="VERIFIED", rules_error=None)
                rule_ok = True
            elif retry_due(rules_attempted, retry_hours, now):
                stamp = now_utc().isoformat()
                print(f"PREP [{index}/{len(events)}] {event_id} | verifying rules", flush=True)
                update_state(db, event_id, rules_status="RUNNING", rules_attempted_at=stamp, rules_error=None)
                ok, output = run_script(verify_script, config, event_id)
                if ok:
                    update_state(db, event_id, rules_status="VERIFIED", rules_error=None)
                    print(f"PREP {event_id} | rules VERIFIED", flush=True)
                    rule_ok = True
                else:
                    update_state(db, event_id, rules_status="FAILED", rules_error=output)
                    print(f"PREP {event_id} | rules FAILED | {output.splitlines()[-1] if output else 'unknown'}", flush=True)
                    rule_ok = False
            else:
                rule_ok = False

            if not rule_ok:
                continue

            if calibration_ready(db, event_id, days, now):
                update_state(db, event_id, calibration_status="CALIBRATED", calibration_error=None)
                continue
            if not retry_due(calibration_attempted, retry_hours, now):
                continue

            stamp = now_utc().isoformat()
            print(f"PREP [{index}/{len(events)}] {event_id} | calibrating {days} days", flush=True)
            update_state(
                db,
                event_id,
                calibration_status="RUNNING",
                calibration_attempted_at=stamp,
                calibration_error=None,
            )
            ok, output = run_script(
                calibrate_script,
                config,
                event_id,
                ["--days", str(days), "--history-days", "15"],
            )
            if ok and calibration_ready(db, event_id, days, now_utc()):
                update_state(db, event_id, calibration_status="CALIBRATED", calibration_error=None)
                print(f"PREP {event_id} | calibration CALIBRATED", flush=True)
            else:
                error = output if not ok else "calibrate.py completed but required model/day coverage is still missing"
                update_state(db, event_id, calibration_status="FAILED", calibration_error=error)
                print(f"PREP {event_id} | calibration FAILED | {error.splitlines()[-1] if error else 'unknown'}", flush=True)


if __name__ == "__main__":
    main()
