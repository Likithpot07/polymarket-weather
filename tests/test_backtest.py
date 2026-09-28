import sqlite3
import unittest
from datetime import datetime, timezone

from backtest import analyze, quote_payoffs
from dataset import build, cycle_times, validate_event_labels
from collector import DDL, save_event
from filters import Policy


class BacktestTest(unittest.TestCase):
    def test_cycle_uses_last_model_receipt_not_first(self):
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        for i, name in enumerate(("ecmwf", "gfs", "icon")):
            stamp = f"2026-09-24T18:00:{i:02d}+00:00"
            db.execute("""INSERT INTO forecast_batches
                (id,city,model,fetched_at,model_run_at,endpoint,raw_json)
                VALUES (?, ?, ?, ?, NULL, '', '{}')""",
                       (i + 1, "NYC", name, stamp))
            db.execute("INSERT INTO forecast_values VALUES (?, 'daily_max', '2026-09-25', 20)", (i + 1,))
        times = cycle_times(db, "NYC", "2026-09-25", {"ecmwf", "gfs", "icon"}, 60, 10)
        self.assertEqual(times, [datetime(2026, 9, 24, 18, 0, 2, tzinfo=timezone.utc)])
        db.close()

    def test_quote_payoff_does_not_count_watch_or_assume_fill(self):
        base = {"event_id": "x", "city": "NYC", "target_date": "2026-09-25",
                "market_id": "m1", "side": "NO", "model_probability": "0.8",
                "as_of": "2026-09-24T12:00:00Z", "action": "WATCH", "won": "1",
                "entry_cost": "0.4", "net_edge": "0.5", "depth_verified": "False",
                "rules_verified": "True", "pre_target_day": "True"}
        self.assertEqual(quote_payoffs([base], {"x"})["events_with_quoted_entry"], 0)
        trade = {**base, "action": "TRADE", "depth_verified": "True"}
        self.assertAlmostEqual(quote_payoffs([base, trade], {"x"})["quoted_payoff_per_share_sum"], .6)

    def test_quote_payoff_replays_live_deterioration_gate(self):
        rows = [
            {
                "event_id": "x", "city": "NYC", "target_date": "2026-09-25",
                "market_id": "m1", "side": "NO", "model_probability": "0.80",
                "as_of": "2026-09-24T12:00:00Z", "action": "TRADE", "won": "1",
                "entry_cost": "0.40", "net_edge": "0.30", "depth_verified": "True",
                "rules_verified": "True", "pre_target_day": "True",
            },
            {
                "event_id": "x", "city": "NYC", "target_date": "2026-09-25",
                "market_id": "m2", "side": "NO", "model_probability": "0.75",
                "as_of": "2026-09-24T12:00:00Z", "action": "TRADE", "won": "0",
                "entry_cost": "0.50", "net_edge": "0.20", "depth_verified": "True",
                "rules_verified": "True", "pre_target_day": "True",
            },
            {
                "event_id": "x", "city": "NYC", "target_date": "2026-09-25",
                "market_id": "m1", "side": "NO", "model_probability": "0.60",
                "as_of": "2026-09-24T18:00:00Z", "action": "WATCH", "won": "1",
                "entry_cost": "0.45", "net_edge": "0.02", "depth_verified": "True",
                "rules_verified": "True", "pre_target_day": "True",
            },
            {
                "event_id": "x", "city": "NYC", "target_date": "2026-09-25",
                "market_id": "m2", "side": "NO", "model_probability": "0.80",
                "as_of": "2026-09-24T18:00:00Z", "action": "TRADE", "won": "0",
                "entry_cost": "0.50", "net_edge": "0.30", "depth_verified": "True",
                "rules_verified": "True", "pre_target_day": "True",
            },
        ]
        result = quote_payoffs(rows, {"x"})
        self.assertEqual(result["quoted_entries"], 2)
        self.assertEqual(result["wins"], 1)
        self.assertAlmostEqual(result["quoted_payoff_per_share_sum"], 0.10)


    def test_holdout_labels_cannot_change_fitted_shrinkage(self):
        rows = []
        for day in range(1, 11):
            eid = str(day)
            for market, p, won in (("a", .8, 1), ("b", .2, 0)):
                rows.append({"event_id": eid, "target_date": f"2026-09-{day:02d}",
                             "market_id": market, "side": "YES", "as_of": f"2026-08-{day:02d}T12:00:00Z",
                             "pre_target_day": "True", "rules_verified": "True",
                             "model_probability": str(p), "resolved_yes": str(won),
                             "action": "WATCH", "won": str(won), "entry_cost": "", "net_edge": "",
                             "depth_verified": "False"})
        original = analyze(rows, min_train_events=1, min_test_events=1,
                           min_train_dates=1, min_test_dates=1)
        changed = [{**r, "resolved_yes": str(1 - int(r["resolved_yes"]))}
                   if int(r["event_id"]) > 7 else r for r in rows]
        altered = analyze(changed, min_train_events=1, min_test_events=1,
                          min_train_dates=1, min_test_dates=1)
        self.assertEqual(original["train_fitted_uniform_shrinkage"], altered["train_fitted_uniform_shrinkage"])
        self.assertNotEqual(original["holdout"]["raw_brier_per_event"], altered["holdout"]["raw_brier_per_event"])
        self.assertLess(original["split"]["train_end_date"], original["split"]["holdout_start_date"])

    def test_missing_labels_are_insufficient(self):
        self.assertEqual(analyze([])["status"], "INSUFFICIENT_VERIFIED_HISTORY")

    def test_build_replays_as_of_and_joins_verified_outcome(self):
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        save_event(db, {"id": "event1", "slug": "nyc-2026-09-25",
                        "title": "Highest temperature in NYC on September 25?",
                        "markets": [
                            {"id": "low", "question": "Will it be 69°F or below?",
                             "outcomes": '["Yes","No"]', "clobTokenIds": '["lo-y","lo-n"]'},
                            {"id": "high", "question": "Will it be 70°F or higher?",
                             "outcomes": '["Yes","No"]', "clobTokenIds": '["hi-y","hi-n"]'}]})
        db.execute("INSERT INTO locations VALUES ('NYC',40,-73,'America/New_York','test','{}')")
        db.execute("""UPDATE rule_reviews SET station='KLGA',unit='F',verified=1,
                    reviewed_at='2026-09-23T10:00:00+00:00'""")
        for i, model in enumerate(("ecmwf", "gfs", "icon")):
            db.execute("""INSERT INTO forecast_batches
                (id,city,model,fetched_at,model_run_at,endpoint,raw_json)
                VALUES (?, 'NYC', ?, ?, NULL, '', '{}')""",
                       (i + 1, model, f"2026-09-24T18:00:0{i}+00:00"))
            db.execute("INSERT INTO forecast_values VALUES (?, 'daily_max', '2026-09-25', 21)", (i + 1,))
        for i, model in enumerate(("ecmwf", "gfs", "icon"), 4):
            db.execute("""INSERT INTO forecast_batches
                (id,city,model,fetched_at,model_run_at,endpoint,raw_json)
                VALUES (?, 'NYC', ?, ?, NULL, '', '{}')""",
                       (i, model, f"2026-09-25T12:00:0{i - 4}+00:00"))
            db.execute("INSERT INTO forecast_values VALUES (?, 'daily_max', '2026-09-25', 21)", (i,))
        for token in ("lo-y", "lo-n", "hi-y", "hi-n"):
            book = '{"bids":[{"price":"0.3","size":"10"}],"asks":[{"price":"0.4","size":"10"}]}'
            db.execute("""INSERT INTO book_events(token_id,event_type,received_at,raw_json)
                VALUES (?, 'book', '2026-09-24T18:00:01+00:00', ?)""", (token, book))
        labels = {"event1": {mid: {"yes_result": result, "source_url": "https://example.org/verified",
                                   "verified_at": "2026-09-26T12:00:00+00:00"}
                              for mid, result in (("low", 0), ("high", 1))}}
        validate_event_labels(db, "event1", "2026-09-25", labels)
        rows, skips = build(db, ["event1"], {"weather_models": ["ecmwf", "gfs", "icon"]},
                            Policy(), labels)
        self.assertEqual(len(rows), 8)
        self.assertEqual(rows[0]["as_of"], "2026-09-24T18:00:02+00:00")
        self.assertEqual(sum(not r["pre_target_day"] for r in rows), 4)
        self.assertEqual({r["won"] for r in rows}, {0, 1})
        self.assertTrue(all(r["rules_verified"] for r in rows))
        self.assertFalse(skips)
        db.close()


if __name__ == "__main__":
    unittest.main()
