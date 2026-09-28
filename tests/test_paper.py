import sqlite3
import unittest
from datetime import datetime, timezone

from paper import PAPER_DDL, city_day_gate, schedule_order, settled_market_results, simulate_buy_fill


class PaperTradingTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(PAPER_DDL)
        self.db.execute(
            """CREATE TABLE markets (
                   id TEXT PRIMARY KEY,
                   city TEXT,
                   target_date TEXT
               )"""
        )
        self.db.execute(
            "INSERT INTO markets(id,city,target_date) VALUES ('m1','NYC','2026-09-28')"
        )
        self.cfg = {
            "paper": {
                "enabled": True,
                "notional_usd": 10.0,
                "latency_ms": 1000,
                "max_slippage_per_share": 0.03,
                "max_book_age_seconds": 10,
                "settlement_check_seconds": 300,
                "max_trades_per_city_day": 3,
                "deterioration_probability_drop": 0.15,
            }
        }

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


    def test_schedule_order_rejects_yes(self):
        state = {"event_id": "e1", "city": "NYC", "target_date": "2026-09-28"}
        decision = {
            "market_id": "m1",
            "side": "YES",
            "action": "TRADE",
            "token_id": "t1",
            "bracket": "test",
            "best_ask": 0.50,
            "model_probability": 0.70,
        }
        order_id = schedule_order(
            self.db,
            state,
            decision,
            datetime(2026, 9, 28, 8, 0, tzinfo=timezone.utc),
            self.cfg,
        )
        self.assertIsNone(order_id)
        self.assertEqual(self.db.execute("SELECT count(*) FROM paper_orders").fetchone()[0], 0)

    def test_city_day_locked_until_latest_fill_deteriorates(self):
        self.db.execute(
            """INSERT INTO paper_orders(
                   event_id,market_id,side,token_id,bracket,signal_at,execute_after,status,
                   model_probability,signal_ask,desired_shares,max_price,filled_shares,
                   vwap,fees,total_cost,fill_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "e1","m1","NO","t1","test",
                "2026-09-28T08:00:00+00:00","2026-09-28T08:00:01+00:00","FILLED",
                0.80,0.50,20.0,0.53,20.0,0.50,0.0,10.0,"2026-09-28T08:00:01+00:00"
            ),
        )
        healthy = [{
            "market_id": "m1",
            "side": "NO",
            "model_probability": 0.72,
            "net_edge": 0.10,
        }]
        allowed, reason = city_day_gate(
            self.db, "NYC", "2026-09-28", healthy, self.cfg
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "CITY_DAY_HEALTHY_POSITION")

        bad = [{
            "market_id": "m1",
            "side": "NO",
            "model_probability": 0.64,
            "net_edge": 0.05,
        }]
        allowed, reason = city_day_gate(
            self.db, "NYC", "2026-09-28", bad, self.cfg
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "LATEST_POSITION_DETERIORATED")

    def test_negative_current_edge_does_not_reopen_healthy_position(self):
        self.db.execute(
            """INSERT INTO paper_orders(
                   event_id,market_id,side,token_id,bracket,signal_at,execute_after,status,
                   model_probability,signal_ask,desired_shares,max_price,filled_shares,
                   vwap,fees,total_cost,fill_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "e1","m1","NO","t1","test",
                "2026-09-28T08:00:00+00:00","2026-09-28T08:00:01+00:00","FILLED",
                0.80,0.50,20.0,0.53,20.0,0.50,0.0,10.0,"2026-09-28T08:00:01+00:00"
            ),
        )
        decisions = [{
            "market_id": "m1",
            "side": "NO",
            "model_probability": 0.76,
            "net_edge": -0.05,
        }]
        allowed, reason = city_day_gate(
            self.db, "NYC", "2026-09-28", decisions, self.cfg
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "CITY_DAY_HEALTHY_POSITION")

    def test_city_day_stops_at_max_trades(self):
        for i in range(3):
            self.db.execute(
                """INSERT INTO paper_orders(
                       event_id,market_id,side,token_id,bracket,signal_at,execute_after,status,
                       model_probability,signal_ask,desired_shares,max_price,filled_shares,
                       vwap,fees,total_cost,fill_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "e1","m1","NO",f"t{i}","test",
                    f"2026-09-28T08:0{i}:00+00:00",f"2026-09-28T08:0{i}:01+00:00","FILLED",
                    0.80,0.50,20.0,0.53,20.0,0.50,0.0,10.0,f"2026-09-28T08:0{i}:01+00:00"
                ),
            )
        decisions = [{
            "market_id": "m1",
            "side": "NO",
            "model_probability": 0.50,
            "net_edge": -0.10,
        }]
        allowed, reason = city_day_gate(
            self.db, "NYC", "2026-09-28", decisions, self.cfg
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "CITY_DAY_MAX_TRADES")

    def test_gamma_outcome_prices_map_to_yes_result(self):
        event = {
            "closed": True,
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

    def test_gamma_prices_do_not_settle_open_event(self):
        event = {
            "closed": False,
            "markets": [
                {
                    "id": "m1",
                    "outcomes": ["Yes", "No"],
                    "outcomePrices": ["1", "0"],
                }
            ],
        }
        self.assertEqual(settled_market_results(event), {})



if __name__ == "__main__":
    unittest.main()
