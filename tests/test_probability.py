import sqlite3
import unittest
from datetime import datetime, timezone

from collector import DDL, save_event
from filters import Policy, assess
from probability import DDL as PROBABILITY_DDL, evaluate


class InDayObservationTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(DDL)
        self.db.executescript(PROBABILITY_DDL)
        save_event(self.db, {
            "id": "event1",
            "slug": "highest-temperature-in-nyc-on-september-28-2026",
            "title": "Highest temperature in NYC on September 28?",
            "markets": [
                {"id": "low", "question": "Will the highest temperature in NYC be 59°F or below?",
                 "outcomes": '["Yes","No"]', "clobTokenIds": '["low-y","low-n"]'},
                {"id": "mid", "question": "Will the highest temperature in NYC be between 60-61°F?",
                 "outcomes": '["Yes","No"]', "clobTokenIds": '["mid-y","mid-n"]'},
                {"id": "high", "question": "Will the highest temperature in NYC be 62°F or higher?",
                 "outcomes": '["Yes","No"]', "clobTokenIds": '["high-y","high-n"]'},
            ],
        })
        self.db.execute("INSERT INTO locations VALUES ('NYC',40,-73,'America/New_York','test','{}')")
        self.db.execute("UPDATE rule_reviews SET station='KLGA',unit='F',verified=1")
        for i, (model, temp_c) in enumerate((("ecmwf_ifs025", 16.0), ("gfs_global", 15.5), ("icon_global", 16.5)), 1):
            self.db.execute(
                """INSERT INTO forecast_batches
                   (id,city,model,fetched_at,model_run_at,endpoint,raw_json)
                   VALUES (?, 'NYC', ?, '2026-09-28T05:00:00+00:00', NULL, '', '{}')""",
                (i, model),
            )
            self.db.execute(
                "INSERT INTO forecast_values VALUES (?, 'daily_max', '2026-09-28', ?)",
                (i, temp_c),
            )
        for market_id in ("low", "mid", "high"):
            self.db.execute(
                """INSERT INTO observations(
                       market_id,station,observed_at,temperature,unit,source_url,
                       source_record_id,fetched_at,raw_json
                   ) VALUES (?,?,?,?,?,?,?,?,?)""",
                (market_id, "KLGA", "2026-09-28T06:00:00+00:00", 62.2, "F",
                 "https://aviationweather.gov", "KLGA:1",
                 "2026-09-28T06:01:00+00:00", "{}"),
            )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_target_day_observed_high_conditions_probabilities(self):
        state = evaluate(
            self.db,
            "event1",
            datetime(2026, 9, 28, 7, 0, tzinfo=timezone.utc),
            persist=False,
        )
        self.assertTrue(state["in_day_observations_modeled"])
        self.assertEqual(state["in_day_observation"]["high"], 62.2)
        probs = {row["market_id"]: row["p_yes"] for row in state["probabilities"]}
        self.assertEqual(probs["low"], 0.0)
        self.assertEqual(probs["mid"], 0.0)
        self.assertAlmostEqual(sum(probs.values()), 1.0, places=10)

    def test_target_day_warning_removed_when_observation_is_modeled(self):
        state = evaluate(
            self.db,
            "event1",
            datetime(2026, 9, 28, 7, 0, tzinfo=timezone.utc),
            persist=False,
        )
        outcome = next(row for row in state["probabilities"] if row["market_id"] == "high")
        quote = {
            "best_bid": 0.4,
            "best_ask": 0.41,
            "ask_size": 100.0,
            "book_age_seconds": 1.0,
            "quote_source": "book",
            "depth_verified": True,
        }
        policy = Policy(
            require_verified_rules=False,
            min_calibration_days=0,
            min_net_edge=0,
            slippage_per_share=0,
            weather_fee_rate=0,
        )
        decision = assess(
            state, outcome, "YES", quote,
            datetime(2026, 9, 28, 7, 0, tzinfo=timezone.utc),
            "America/New_York", True, policy,
        )
        self.assertNotIn("IN_DAY_OBSERVATIONS_NOT_MODELED", decision["reasons"])


if __name__ == "__main__":
    unittest.main()
