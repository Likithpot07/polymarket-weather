"""Build as-of weather/market snapshots for Step 4. No trades or inferred labels."""

from __future__ import annotations



import argparse

import csv

from collections import Counter

from datetime import date, datetime, time, timezone

import json

from pathlib import Path

from time import perf_counter

import sqlite3

from zoneinfo import ZoneInfo



from decide import event_decisions, verified_at

from filters import Policy

from probability import VERSION, parse_time





FIELDS = (

    "event_id", "city", "target_date", "market_id", "side", "as_of",

    "pre_target_day", "model_version", "model_probability", "model_spread_c",

    "components_json", "station", "unit", "action", "reasons", "token_id",

    "best_bid", "best_ask", "ask_size", "book_age_seconds", "quote_source",

    "depth_verified", "fee_per_share", "slippage_per_share", "entry_cost",

    "net_edge", "rules_verified", "resolved_yes", "won", "label_source_url",

    "label_verified_at",

)





def cycle_times(db: sqlite3.Connection, city: str, target: str,

                models: set[str], gap: float, limit: int) -> list[datetime]:

    """Use the LAST model's receipt time in each complete forecast cycle."""

    rows = db.execute("""SELECT b.fetched_at,b.model FROM forecast_batches b

        JOIN forecast_values v ON v.batch_id=b.id

        WHERE b.city=? AND v.period='daily_max' AND v.valid_time=?

        ORDER BY b.fetched_at,b.id""", (city, target)).fetchall()

    groups: list[dict] = []

    for stamp, model in rows:

        when = parse_time(stamp)

        if groups and (when - groups[-1]["start"]).total_seconds() <= gap:

            groups[-1]["end"] = when

            groups[-1]["models"].add(model)

        else:

            groups.append({"start": when, "end": when, "models": {model}})

    return [g["end"] for g in groups if models.issubset(g["models"])][:limit]





def load_labels(path: Path | None) -> dict[str, dict[str, dict]]:

    """Read separately verified final outcomes; never infer truth from live prices."""

    if path is None:

        return {}

    labels: dict[str, dict[str, dict]] = {}

    with path.open(newline="", encoding="utf-8-sig") as file:

        reader = csv.DictReader(file)

        required = {"event_id", "market_id", "yes_result", "source_url", "verified_at"}

        if not reader.fieldnames or not required.issubset(reader.fieldnames):

            raise ValueError("Labels need: " + ", ".join(sorted(required)))

        for line, row in enumerate(reader, 2):

            eid, mid = row["event_id"].strip(), row["market_id"].strip()

            value, source = row["yes_result"].strip(), row["source_url"].strip()

            if not eid or not mid or value not in ("0", "1") or not source.startswith(("https://", "http://")):

                raise ValueError(f"Invalid verified label at line {line}")

            stamp = parse_time(row["verified_at"].strip())

            event = labels.setdefault(eid, {})

            if mid in event:

                raise ValueError(f"Duplicate label: event {eid}, market {mid}")

            event[mid] = {"yes_result": int(value), "source_url": source,

                          "verified_at": stamp.isoformat()}

    return labels





def validate_event_labels(db: sqlite3.Connection, eid: str, target: str,

                          labels: dict[str, dict[str, dict]]) -> dict[str, dict]:

    event = labels.get(eid, {})

    if not event:

        return {}

    market_ids = {row[0] for row in db.execute("SELECT id FROM markets WHERE event_id=?", (eid,))}

    if set(event) != market_ids or sum(x["yes_result"] for x in event.values()) != 1:

        raise ValueError(f"Event {eid}: labels must cover every bracket with exactly one YES winner")

    city = db.execute("SELECT city FROM markets WHERE event_id=? LIMIT 1", (eid,)).fetchone()[0]

    tz_row = db.execute("SELECT timezone FROM locations WHERE city=?", (city,)).fetchone()

    if not tz_row:

        raise ValueError(f"Event {eid}: timezone missing for verified labels")

    if any(parse_time(x["verified_at"]).astimezone(ZoneInfo(tz_row[0])).date()

           <= date.fromisoformat(target) for x in event.values()):

        raise ValueError(f"Event {eid}: label verification predates end of local target day")

    return event





def build(db: sqlite3.Connection, events: list[str], cfg: dict, policy: Policy,
          labels: dict[str, dict[str, dict]], max_cycles: int = 200,
          cycle_gap: float = 60) -> tuple[list[dict], Counter]:
    rows: list[dict] = []
    skips: Counter = Counter()
    total_events = len(events)

    print(f"Building dataset for {total_events} event(s)...", flush=True)

    for event_no, eid in enumerate(events, 1):
        event_started = perf_counter()
        event_row_start = len(rows)
        event_skips_before = skips.copy()

        info = db.execute(
            "SELECT city,target_date FROM markets WHERE event_id=? LIMIT 1",
            (eid,),
        ).fetchone()

        if not info or not all(info):
            skips["MISSING_EVENT_DATE"] += 1
            print(
                f"[{event_no}/{total_events}] Event {eid}: skipped (MISSING_EVENT_DATE)",
                flush=True,
            )
            continue

        city, target = info
        print(
            f"[{event_no}/{total_events}] Event {eid} | {city} | {target}",
            flush=True,
        )

        event_labels = validate_event_labels(db, eid, target, labels)
        location = db.execute(
            "SELECT timezone FROM locations WHERE city=?",
            (city,),
        ).fetchone()

        if not location:
            skips["MISSING_TIMEZONE"] += 1
            print("  skipped: MISSING_TIMEZONE", flush=True)
            continue

        start_day = datetime.combine(
            date.fromisoformat(target),
            time.min,
            ZoneInfo(location[0]),
        ).astimezone(timezone.utc)

        cycle_started = perf_counter()
        times = cycle_times(
            db,
            city,
            target,
            set(cfg["weather_models"]),
            cycle_gap,
            max_cycles,
        )
        print(
            f"  complete cycles: {len(times)} "
            f"(lookup {perf_counter() - cycle_started:.2f}s)",
            flush=True,
        )

        if not times:
            skips["NO_COMPLETE_FORECAST_CYCLE"] += 1
            print("  skipped: NO_COMPLETE_FORECAST_CYCLE", flush=True)
            continue

        # The rule-review rows are event-level. Fetch them once instead of once
        # for every historical cycle. verified_at() is still evaluated at each
        # as_of timestamp, so the original as-of behavior is preserved.
        rule_rows = db.execute(
            """SELECT r.verified,r.reviewed_at FROM rule_reviews r
               JOIN markets m ON m.id=r.market_id WHERE m.event_id=?""",
            (eid,),
        ).fetchall()

        pre_target_times = [as_of for as_of in times if as_of < start_day]
        in_day_count = len(times) - len(pre_target_times)
        if in_day_count:
            skips["IN_DAY_CYCLE"] += in_day_count
            print(f"  in-day cycles skipped: {in_day_count}", flush=True)

        if not pre_target_times:
            print(
                f"  snapshots: 0 | rows: 0 | elapsed: "
                f"{perf_counter() - event_started:.2f}s",
                flush=True,
            )
            continue

        print(
            f"  processing {len(pre_target_times)} pre-target cycle(s)...",
            flush=True,
        )

        for cycle_no, as_of in enumerate(pre_target_times, 1):
            cycle_work_started = perf_counter()
            before = len(rows)

            try:
                state, decisions = event_decisions(db, eid, as_of, policy)
            except ValueError:
                skips["INCOMPLETE_PROBABILITY"] += 1
                print(
                    f"    [{cycle_no}/{len(pre_target_times)}] "
                    f"{as_of.isoformat()} -> INCOMPLETE_PROBABILITY "
                    f"({perf_counter() - cycle_work_started:.2f}s)",
                    flush=True,
                )
                continue

            rules_verified = bool(rule_rows) and all(
                verified_at(flag, reviewed, as_of)
                for flag, reviewed in rule_rows
            )

            for d in decisions:
                label = event_labels.get(d["market_id"])
                won = (
                    label["yes_result"]
                    if d["side"] == "YES"
                    else 1 - label["yes_result"]
                ) if label else None

                rows.append({
                    "event_id": eid, "city": city, "target_date": target,
                    "market_id": d["market_id"], "side": d["side"],
                    "as_of": as_of.isoformat(), "pre_target_day": True,
                    "model_version": VERSION,
                    "model_probability": d["model_probability"],
                    "model_spread_c": d.get("model_spread_c"),
                    "components_json": json.dumps(
                        state["components"], separators=(",", ":")
                    ),
                    "station": state["station"], "unit": state["unit"],
                    "action": d["action"],
                    "reasons": ",".join(d["reasons"]),
                    "token_id": d["token_id"],
                    "best_bid": d["best_bid"],
                    "best_ask": d["best_ask"],
                    "ask_size": d["ask_size"],
                    "book_age_seconds": d["book_age_seconds"],
                    "quote_source": d["quote_source"],
                    "depth_verified": d["depth_verified"],
                    "fee_per_share": d["fee_per_share"],
                    "slippage_per_share": d["slippage_per_share"],
                    "entry_cost": d["entry_cost"],
                    "net_edge": d["net_edge"],
                    "rules_verified": rules_verified,
                    "resolved_yes": label["yes_result"] if label else None,
                    "won": won,
                    "label_source_url": label["source_url"] if label else None,
                    "label_verified_at": label["verified_at"] if label else None,
                })

            produced = len(rows) - before
            print(
                f"    [{cycle_no}/{len(pre_target_times)}] "
                f"{as_of.isoformat()} -> {produced} row(s) "
                f"({perf_counter() - cycle_work_started:.2f}s)",
                flush=True,
            )

        event_skip_delta = {
            key: skips[key] - event_skips_before[key]
            for key in skips
            if skips[key] - event_skips_before[key]
        }
        event_rows = len(rows) - event_row_start
        print(
            f"  event complete: {event_rows} row(s) | "
            f"skips: {event_skip_delta or {}} | "
            f"elapsed: {perf_counter() - event_started:.2f}s",
            flush=True,
        )

    return rows, skips



def main() -> None:

    ap = argparse.ArgumentParser(description=__doc__)

    ap.add_argument("--config", default="config.json")

    ap.add_argument("--event-id", help="Only this event (default: all recorded events)")

    ap.add_argument("--labels", help="CSV with verified final YES outcomes, one per bracket")

    ap.add_argument("--out", default="data/step4_dataset.csv")

    ap.add_argument("--max-cycles", type=int, default=200)

    ap.add_argument("--cycle-gap", type=float, default=60)

    args = ap.parse_args()

    if args.max_cycles <= 0 or args.cycle_gap < 0:

        ap.error("max-cycles must be positive and cycle-gap nonnegative")

    config_path = Path(args.config).resolve()

    cfg = json.loads(config_path.read_text(encoding="utf-8"))

    root = config_path.parent

    db_path = Path(cfg["db_path"])

    db_path = db_path if db_path.is_absolute() else root / db_path

    if not db_path.is_file():

        ap.error(f"Collector database missing: {db_path}")

    try:

        labels = load_labels(Path(args.labels)) if args.labels else {}

        with sqlite3.connect(db_path) as db:

            events = [args.event_id] if args.event_id else [r[0] for r in db.execute(

                "SELECT id FROM events ORDER BY id")]

            rows, skips = build(db, events, cfg, Policy.from_config(cfg), labels,

                                args.max_cycles, args.cycle_gap)

    except (ValueError, sqlite3.Error) as exc:

        ap.error(str(exc))

    out = Path(args.out)

    out = out if out.is_absolute() else root / out

    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(file, fieldnames=FIELDS)

        writer.writeheader()

        writer.writerows(rows)

    print(f"Events: {len(events)} | snapshots: {len({(r['event_id'], r['as_of']) for r in rows})} "

          f"| decision rows: {len(rows)} | labeled events: {len({r['event_id'] for r in rows if r['resolved_yes'] is not None})}")

    print("Skipped:", dict(skips))

    print("Dataset:", out)

    if not labels:

        print("Outcome labels missing; collect and verify resolutions before evaluating calibration or returns.")





if __name__ == "__main__":

    main()
