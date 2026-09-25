"""Step 4: date-split calibration and hypothetical quote returns on verified labels."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path


def complete_events(rows: list[dict]) -> tuple[dict[str, list[dict]], Counter]:
    """One latest pre-target snapshot per event, with every bracket labeled."""
    snapshots: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if row["side"] == "YES" and row["pre_target_day"] == "True":
            snapshots[row["event_id"]][row["as_of"]].append(row)
    excluded = Counter()
    result = {}
    for eid, times in snapshots.items():
        selected = times[max(times)]
        if any(r["rules_verified"] != "True" for r in selected):
            excluded["RULES_UNVERIFIED"] += 1
            continue
        if not selected or any(r["resolved_yes"] not in ("0", "1") for r in selected):
            excluded["MISSING_VERIFIED_OUTCOMES"] += 1
            continue
        if sum(int(r["resolved_yes"]) for r in selected) != 1:
            excluded["INVALID_OUTCOMES"] += 1
            continue
        if abs(sum(float(r["model_probability"]) for r in selected) - 1) > 1e-6:
            excluded["INCOMPLETE_PROBABILITIES"] += 1
            continue
        result[eid] = selected
    return result, excluded


def event_brier(event: list[dict], alpha: float = 0) -> float:
    uniform = 1 / len(event)
    return sum(((1 - alpha) * float(r["model_probability"]) + alpha * uniform
                - int(r["resolved_yes"])) ** 2 for r in event)


def event_log_loss(event: list[dict], alpha: float = 0) -> float:
    uniform = 1 / len(event)
    winner = next(r for r in event if r["resolved_yes"] == "1")
    p = (1 - alpha) * float(winner["model_probability"]) + alpha * uniform
    return -math.log(max(1e-12, p))


def fit_shrinkage(events: list[list[dict]]) -> float:
    """Choose sharpness on training events only; zero preserves the baseline."""
    return min((step / 20 for step in range(21)),
               key=lambda a: (sum(event_brier(e, a) for e in events), a))


def quote_payoffs(rows: list[dict], event_ids: set[str]) -> dict:
    """At most one earliest permitted entry per event; no fill is presumed."""
    trades: dict[str, dict] = {}
    for r in sorted(rows, key=lambda x: (x["as_of"], x["event_id"],
                                         -float(x["net_edge"] or 0))):
        eid = r["event_id"]
        if (eid in event_ids and eid not in trades and r["action"] == "TRADE"
                and r["won"] in ("0", "1") and r["entry_cost"]
                and r["depth_verified"] == "True" and r["rules_verified"] == "True"
                and r["pre_target_day"] == "True"):
            trades[eid] = r
    payoffs = [int(r["won"]) - float(r["entry_cost"]) for r in trades.values()]
    return {"events_with_quoted_entry": len(trades),
            "wins": sum(int(r["won"]) for r in trades.values()),
            "quoted_payoff_per_share_sum": round(sum(payoffs), 6),
            "quoted_payoff_per_share_mean": round(sum(payoffs) / len(payoffs), 6) if payoffs else None,
            "fill_assumption": "one share at recorded ask; no actual fills recorded"}


def reliability(events: list[list[dict]], alpha: float) -> list[dict]:
    bins: dict[int, list[tuple[float, int]]] = defaultdict(list)
    for event in events:
        uniform = 1 / len(event)
        for row in event:
            p = (1 - alpha) * float(row["model_probability"]) + alpha * uniform
            bins[min(9, int(10 * p))].append((p, int(row["resolved_yes"])))
    return [{"p_range": [i / 10, (i + 1) / 10], "brackets": len(v),
             "mean_prediction": round(sum(p for p, _ in v) / len(v), 5),
             "observed_frequency": round(sum(y for _, y in v) / len(v), 5)}
            for i, v in sorted(bins.items())]


def analyze(rows: list[dict], test_fraction: float = .3,
            min_train_events: int = 20, min_test_events: int = 10,
            min_train_dates: int = 5, min_test_dates: int = 2) -> dict:
    events, excluded = complete_events(rows)
    days = sorted({e[0]["target_date"] for e in events.values()})
    report = {"status": "INSUFFICIENT_VERIFIED_HISTORY", "dataset_rows": len(rows),
              "verified_events": len(events), "distinct_target_dates": len(days),
              "excluded_events": dict(excluded), "train": None, "holdout": None}
    if len(days) < 2:
        return report
    cutoff = min(len(days) - 1, max(1, math.ceil(len(days) * (1 - test_fraction))))
    train_days, test_days = set(days[:cutoff]), set(days[cutoff:])
    train_ids = {eid for eid, e in events.items() if e[0]["target_date"] in train_days}
    test_ids = set(events) - train_ids
    report["split"] = {"train_end_date": max(train_days), "holdout_start_date": min(test_days),
                       "train_dates": len(train_days), "holdout_dates": len(test_days)}
    train_events, test_events = ([events[eid] for eid in sorted(group)] for group in (train_ids, test_ids))
    alpha = fit_shrinkage(train_events)
    for name, group, ids in (("train", train_events, train_ids), ("holdout", test_events, test_ids)):
        report[name] = {
            "events": len(group),
            "raw_brier_per_event": round(sum(event_brier(e) for e in group) / len(group), 6),
            "calibrated_brier_per_event": round(sum(event_brier(e, alpha) for e in group) / len(group), 6),
            "raw_log_loss": round(sum(event_log_loss(e) for e in group) / len(group), 6),
            "calibrated_log_loss": round(sum(event_log_loss(e, alpha) for e in group) / len(group), 6),
            "reliability": reliability(group, alpha), "quoted_entries": quote_payoffs(rows, ids),
        }
    report["train_fitted_uniform_shrinkage"] = alpha
    if (len(train_ids) >= min_train_events and len(test_ids) >= min_test_events
            and len(train_days) >= min_train_dates and len(test_days) >= min_test_dates):
        entries = report["holdout"]["quoted_entries"]
        report["status"] = ("RESEARCH_ONLY_FILL_UNVERIFIED" if
                            entries["quoted_payoff_per_share_sum"] > 0 and
                            entries["events_with_quoted_entry"] > 0 else "EDGE_NOT_VALIDATED")
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", default="data/step4_dataset.csv")
    ap.add_argument("--out", default="data/step4_report.json")
    ap.add_argument("--test-fraction", type=float, default=.3)
    args = ap.parse_args()
    if not 0 < args.test_fraction < 1:
        ap.error("test-fraction must be between zero and one")
    try:
        with Path(args.dataset).open(newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
        required = {"event_id", "target_date", "market_id", "side", "as_of", "pre_target_day",
                    "model_probability", "resolved_yes", "rules_verified", "action", "won",
                    "entry_cost", "net_edge", "depth_verified"}
        if rows and not required.issubset(rows[0]):
            raise ValueError("Dataset is missing required Step 4 fields")
        result = analyze(rows, args.test_fraction)
    except (ValueError, OSError) as exc:
        ap.error(str(exc))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print("Report:", out)


if __name__ == "__main__":
    main()
