import sqlite3
import unittest
from datetime import datetime, timezone

from live import LIVE_DDL, persist_snapshot


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


if __name__ == "__main__":
    unittest.main()
