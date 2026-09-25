import sqlite3
import unittest

from collector import DDL, extract_date, record_frame, save_event


class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(DDL)

    def tearDown(self):
        self.db.close()

    def test_raw_gamma_tokens_and_rule_review(self):
        event = {"id": "1", "slug": "highest-temperature-in-london-on-september-25-2026",
                 "title": "Highest temperature in London on September 25?",
                 "markets": [{"id": "2", "question": "Will London be 31°C?",
                              "description": "Resolution by station XYZ; whole degrees C",
                              "outcomes": '["Yes","No"]', "clobTokenIds": '["101","102"]'}]}
        self.assertEqual(save_event(self.db, event), {"101": "2", "102": "2"})
        self.assertEqual(self.db.execute("SELECT city,target_date FROM markets").fetchone(), ("London", "2026-09-25"))
        self.assertEqual(self.db.execute("SELECT verified FROM rule_reviews").fetchone(), (0,))

    def test_unverified_year_stays_unknown(self):
        self.assertIsNone(extract_date("Highest temperature in London on September 25?", "some-slug"))

    def test_full_book_uses_price_extrema_and_keeps_depth(self):
        frame = {"event_type": "book", "asset_id": "101", "timestamp": "1234",
                 "bids": [{"price": "0.08", "size": "3"}, {"price": "0.09", "size": "4"}],
                 "asks": [{"price": "0.99", "size": "5"}, {"price": "0.10", "size": "6"}]}
        self.assertEqual(record_frame(self.db, frame, {"101"}), 1)
        self.assertEqual(self.db.execute("SELECT best_bid,best_ask FROM book_events").fetchone(), ("0.09", "0.1"))
        self.assertEqual(record_frame(self.db, frame, {"other"}), 0)


if __name__ == "__main__":
    unittest.main()
