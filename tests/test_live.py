import sqlite3
import unittest
from datetime import datetime, timezone

from live import LIVE_DDL, persist_candidate_audit, persist_snapshot


class LivePersistenceTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(LIVE_DDL)
        self.state = {
            "event_id": "event1",
            "city": "NYC",
            "target_date": "2026-09-28",
        }
        self.as_of = datetime(2026, 9, 28, 8, 0, tzinfo=timezone.utc)
        self.trade = {
            "market_id": "m1",
            "side": "YES",
            "action": "TRADE",
            "model_probability": 0.70,
            "best_bid": 0.49,
            "best_ask": 0.50,
            "ask_size": 100.0,
            "book_age_seconds": 1.0,
            "quote_source": "book",
            "depth_verified": True,
            "entry_cost": 0.52,
            "net_edge": 0.18,
            "reasons": [],
            "bracket": "test bracket",
            "token_id": "token1",
        }

    def tearDown(self):
        self.db.close()

    def test_trade_signal_is_recorded_only_on_transition(self):
        with self.db:
            first = persist_snapshot(self.db, self.state, [self.trade], self.as_of)
        self.assertEqual(len(first), 1)
        self.assertEqual(
            self.db.execute("SELECT count(*) FROM paper_trades").fetchone()[0],
            1,
        )

        with self.db:
            second = persist_snapshot(self.db, self.state, [self.trade], self.as_of)
        self.assertEqual(second, [])
        self.assertEqual(
            self.db.execute("SELECT count(*) FROM paper_trades").fetchone()[0],
            1,
        )

    def test_trade_signal_closes_when_action_changes(self):
        with self.db:
            persist_snapshot(self.db, self.state, [self.trade], self.as_of)
        watch = {
            **self.trade,
            "action": "WATCH",
            "reasons": ["BELOW_MIN_NET_EDGE"],
            "net_edge": 0.04,
        }
        with self.db:
            persist_snapshot(self.db, self.state, [watch], self.as_of)
        row = self.db.execute(
            "SELECT status,close_reason FROM paper_trades"
        ).fetchone()
        self.assertEqual(row[0], "CLOSED_SIGNAL")
        self.assertIn("BELOW_MIN_NET_EDGE", row[1])

    def test_candidate_selection_reason_is_persisted(self):
        no_trade = {**self.trade, "side": "NO"}
        with self.db:
            persist_snapshot(self.db, self.state, [no_trade], self.as_of)
            persist_candidate_audit(
                self.db,
                self.state,
                [no_trade],
                self.as_of,
                scheduled=[],
                skip_reasons={id(no_trade): "CITY_DAY_HEALTHY_POSITION"},
            )
        row = self.db.execute(
            """SELECT selected,selection_reason,net_edge
               FROM paper_candidate_audit"""
        ).fetchone()
        self.assertEqual(row[0], 0)
        self.assertEqual(row[1], "CITY_DAY_HEALTHY_POSITION")
        self.assertAlmostEqual(row[2], 0.18)



if __name__ == "__main__":
    unittest.main()
