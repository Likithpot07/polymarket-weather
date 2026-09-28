import json
import sqlite3
import unittest
from datetime import datetime, timezone

from collector import DDL
from edge import book_history, quoted_book


class EdgeBookReplayTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(DDL)

    def tearDown(self):
        self.db.close()

    def add(self, token, kind, stamp, raw, bid=None, ask=None):
        self.db.execute(
            """INSERT INTO book_events(
                   token_id,event_type,source_timestamp,received_at,best_bid,best_ask,raw_json
               ) VALUES (?,?,?,?,?,?,?)""",
            (token, kind, stamp, stamp, bid, ask, json.dumps(raw)),
        )
        self.db.commit()

    def test_replays_price_change_and_preserves_ask_depth(self):
        self.add(
            "101", "book", "2026-09-28T12:00:00+00:00",
            {"asset_id": "101",
             "bids": [{"price": "0.49", "size": "10"}],
             "asks": [{"price": "0.52", "size": "5"}, {"price": "0.53", "size": "8"}]},
        )
        self.add(
            "101", "price_change", "2026-09-28T12:00:05+00:00",
            {"price_changes": [{"asset_id": "101", "price": "0.50", "size": "7",
                                "side": "BUY", "best_bid": "0.50", "best_ask": "0.52"}]},
            "0.50", "0.52",
        )
        history = book_history(self.db, {"101"})
        quote = quoted_book(history["101"], datetime(2026, 9, 28, 12, 0, 6, tzinfo=timezone.utc), 120)
        self.assertEqual(quote["quote_source"], "reconstructed_book")
        self.assertTrue(quote["depth_verified"])
        self.assertEqual(quote["best_bid"], 0.50)
        self.assertEqual(quote["best_ask"], 0.52)
        self.assertEqual(quote["ask_size"], 5.0)
        self.assertEqual(quote["book_age_seconds"], 1.0)

    def test_zero_size_removes_price_level(self):
        self.add(
            "101", "book", "2026-09-28T12:00:00+00:00",
            {"asset_id": "101",
             "bids": [{"price": "0.49", "size": "10"}],
             "asks": [{"price": "0.52", "size": "5"}, {"price": "0.53", "size": "8"}]},
        )
        self.add(
            "101", "price_change", "2026-09-28T12:00:05+00:00",
            {"price_changes": [{"asset_id": "101", "price": "0.52", "size": "0",
                                "side": "SELL", "best_bid": "0.49", "best_ask": "0.53"}]},
            "0.49", "0.53",
        )
        history = book_history(self.db, {"101"})
        quote = quoted_book(history["101"], datetime(2026, 9, 28, 12, 0, 6, tzinfo=timezone.utc), 120)
        self.assertTrue(quote["depth_verified"])
        self.assertEqual(quote["best_ask"], 0.53)
        self.assertEqual(quote["ask_size"], 8.0)

    def test_unexplained_new_top_falls_back_to_unverified_quote(self):
        self.add(
            "101", "book", "2026-09-28T12:00:00+00:00",
            {"asset_id": "101",
             "bids": [{"price": "0.49", "size": "10"}],
             "asks": [{"price": "0.52", "size": "5"}]},
        )
        self.add(
            "101", "best_bid_ask", "2026-09-28T12:00:05+00:00",
            {"asset_id": "101", "best_bid": "0.50", "best_ask": "0.51"},
            "0.50", "0.51",
        )
        history = book_history(self.db, {"101"})
        quote = quoted_book(history["101"], datetime(2026, 9, 28, 12, 0, 6, tzinfo=timezone.utc), 120)
        self.assertEqual(quote["quote_source"], "top_of_book")
        self.assertFalse(quote["depth_verified"])
        self.assertIsNone(quote["ask_size"])
        self.assertEqual(quote["best_bid"], 0.50)
        self.assertEqual(quote["best_ask"], 0.51)


if __name__ == "__main__":
    unittest.main()
