import sqlite3
import unittest
from datetime import datetime, timezone

from paper import PAPER_DDL, settled_market_results, simulate_buy_fill


class PaperTradingTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(PAPER_DDL)

    def tearDown(self):
        self.db.close()

    def test_depth_aware_fill_walks_book(self):
        result = simulate_buy_fill(
            [
                {"price": 0.40, "size": 5},
                {"price": 0.41, "size": 10},
                {"price": 0.45, "size": 20},
            ],
            desired_shares=12,
            max_price=0.43,
            fee_rate=0.05,
        )
        self.assertAlmostEqual(result["filled_shares"], 12)
        self.assertAlmostEqual(result["vwap"], (0.40 * 5 + 0.41 * 7) / 12)
        self.assertEqual(result["fills"], [(0.40, 5.0), (0.41, 7.0)])

    def test_slippage_limit_can_partial_fill(self):
        result = simulate_buy_fill(
            [
                {"price": 0.40, "size": 3},
                {"price": 0.44, "size": 20},
            ],
            desired_shares=10,
            max_price=0.42,
            fee_rate=0.05,
        )
        self.assertAlmostEqual(result["filled_shares"], 3)
        self.assertAlmostEqual(result["remaining_shares"], 7)

    def test_gamma_outcome_prices_map_to_yes_result(self):
        event = {
            "markets": [
                {
                    "id": "m1",
                    "outcomes": '["Yes","No"]',
                    "outcomePrices": '["1","0"]',
                },
                {
                    "id": "m2",
                    "outcomes": ["Yes", "No"],
                    "outcomePrices": ["0", "1"],
                },
            ]
        }
        self.assertEqual(settled_market_results(event), {"m1": 1, "m2": 0})


if __name__ == "__main__":
    unittest.main()
