"""Tests for the v4 fixes: in-day max model, locked NO, market blend, trade picker."""
import io
import json
import sqlite3
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from collector import DDL, save_event, station_coordinates
from filters import Policy, assess
from paper import best_no_trade
from probability import DDL as PROBABILITY_DDL, evaluate, observed_floor_probability

NY = ZoneInfo("America/New_York")


def build_db(observations_f, hourly_c_by_local_hour, daily_c=(21.0, 21.0, 21.0)):
    db = sqlite3.connect(":memory:")
    db.executescript(DDL)
    db.executescript(PROBABILITY_DDL)
    save_event(db, {
        "id": "e1",
        "slug": "highest-temperature-in-nyc-on-september-25-2026",
        "title": "Highest temperature in NYC on September 25?",
        "markets": [
            {"id": "m0", "question": "Will the highest temperature in NYC be 65°F or below?",
             "outcomes": '["Yes","No"]', "clobTokenIds": '["m0y","m0n"]'},
            {"id": "m1", "question": "Will the highest temperature in NYC be between 66-67°F?",
             "outcomes": '["Yes","No"]', "clobTokenIds": '["m1y","m1n"]'},
            {"id": "m2", "question": "Will the highest temperature in NYC be between 68-69°F?",
             "outcomes": '["Yes","No"]', "clobTokenIds": '["m2y","m2n"]'},
            {"id": "m3", "question": "Will the highest temperature in NYC be between 70-71°F?",
             "outcomes": '["Yes","No"]', "clobTokenIds": '["m3y","m3n"]'},
            {"id": "m4", "question": "Will the highest temperature in NYC be 72°F or higher?",
             "outcomes": '["Yes","No"]', "clobTokenIds": '["m4y","m4n"]'},
        ],
    })
    db.execute("INSERT INTO locations VALUES ('NYC',40.71,-74.0,'America/New_York','test','{}')")
    db.execute("UPDATE rule_reviews SET station='KLGA',unit='F',verified=1")
    for i, (model, temp_c) in enumerate(zip(("ecmwf_ifs025", "gfs_global", "icon_global"), daily_c), 1):
        db.execute("""INSERT INTO forecast_batches(id,city,model,fetched_at,model_run_at,endpoint,raw_json)
                      VALUES (?, 'NYC', ?, '2026-09-25T10:00:00+00:00', NULL, '', '{}')""", (i, model))
        db.execute("INSERT INTO forecast_values VALUES (?, 'daily_max', '2026-09-25', ?)", (i, temp_c))
        for hour, value in hourly_c_by_local_hour.items():
            stamp = datetime(2026, 9, 25, hour, tzinfo=NY).isoformat()
            db.execute("INSERT INTO forecast_values VALUES (?, 'hourly', ?, ?)", (i, stamp, value))
    for n, (local_hour, temp_f) in enumerate(observations_f):
        observed = datetime(2026, 9, 25, local_hour, 51, tzinfo=NY).astimezone(timezone.utc).isoformat()
        db.execute("""INSERT INTO observations(market_id,station,observed_at,temperature,unit,source_url,
                      source_record_id,fetched_at,raw_json) VALUES (?,?,?,?,?,?,?,?,?)""",
                   ("m0", "KLGA", observed, temp_f, "F", "x", f"KLGA:{n}", observed,
                    '{"lat": 40.779, "lon": -73.88}'))
    db.commit()
    return db


def probs(state):
    return {p["market_id"]: p for p in state["probabilities"]}


class InDayMaxModelTest(unittest.TestCase):
    def test_after_peak_current_bracket_is_near_certain(self):
        # Observed 69.1F at 3pm; forecast says it only cools from here (~18C = 64F).
        db = build_db([(13, 68.0), (14, 69.1), (15, 68.9)], {h: 18.0 for h in range(18, 24)})
        state = evaluate(db, "e1", datetime(2026, 9, 25, 22, 30, tzinfo=NY).astimezone(timezone.utc), persist=False)
        p = probs(state)
        self.assertGreater(p["m2"]["p_yes"], 0.95)       # 68-69 is where the day ends
        self.assertLess(p["m3"]["p_yes"], 0.05)
        self.assertEqual(p["m0"]["p_yes"], 0.0)
        self.assertAlmostEqual(sum(x["p_yes"] for x in p.values()), 1.0, places=9)

    def test_morning_keeps_real_uncertainty(self):
        db = build_db([(7, 60.0), (8, 62.0)], {h: 22.0 for h in range(10, 24)})
        state = evaluate(db, "e1", datetime(2026, 9, 25, 9, 0, tzinfo=NY).astimezone(timezone.utc), persist=False)
        p = probs(state)
        self.assertLess(max(x["p_yes"] for x in p.values()), 0.6)

    def test_locked_needs_two_readings_above_bracket(self):
        single = build_db([(12, 60.0), (13, 71.5)], {h: 18.0 for h in range(18, 24)})
        state = evaluate(single, "e1", datetime(2026, 9, 25, 16, 0, tzinfo=NY).astimezone(timezone.utc), persist=False)
        self.assertFalse(probs(state)["m1"]["locked_no"])  # one spike is not enough
        double = build_db([(12, 70.0), (13, 71.5)], {h: 18.0 for h in range(18, 24)})
        state = evaluate(double, "e1", datetime(2026, 9, 25, 16, 0, tzinfo=NY).astimezone(timezone.utc), persist=False)
        p = probs(state)
        self.assertTrue(p["m1"]["locked_no"])   # 66-67 is dead, both readings >= 68.6
        self.assertTrue(p["m0"]["locked_no"])
        self.assertFalse(p["m2"]["locked_no"])  # 68-69 still possible (70.0 < 69.5 + 1.08)

    def test_final_high_never_below_observed(self):
        c = [{"mean": 0, "sigma": 4.5, "remaining_mean": 60.0, "remaining_sigma": 1.0}]
        self.assertEqual(observed_floor_probability(None, 65, c, 66.2), 0.0)

    def test_station_coordinates_come_from_metar(self):
        db = build_db([(12, 70.0)], {})
        self.assertEqual(station_coordinates(db, "NYC")[:2], (40.779, -73.88))


QUOTE = {"best_bid": 0.59, "best_ask": 0.61, "ask_size": 100.0, "book_age_seconds": 1.0,
         "quote_source": "book", "depth_verified": True, "ask_levels": None}
STATE = {"event_id": "e1", "unit": "F", "target_date": "2026-09-27", "components": [
    {"mean": 70, "sigma": 2.0, "calibration_days": 30, "fetched_at": "2026-09-25T10:00:00+00:00"}] * 3}
AS_OF = datetime(2026, 9, 25, 12, tzinfo=timezone.utc)


class BlendTest(unittest.TestCase):
    def test_edge_uses_blend_of_model_and_market(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, model_weight=0.5,
                        min_net_edge=0.0, weather_fee_rate=0.0, slippage_per_share=0.0,
                        max_fade_market_yes=1.0)
        outcome = {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}
        d = assess(STATE, outcome, "NO", QUOTE, AS_OF, "America/New_York", True, policy)
        self.assertAlmostEqual(d["blended_probability"], 0.75)
        self.assertAlmostEqual(d["net_edge"], 0.14)
        self.assertEqual(d["action"], "TRADE")

    def test_does_not_fade_market_favourite(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, min_net_edge=0.0)
        outcome = {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}
        d = assess(STATE, outcome, "NO", QUOTE, AS_OF, "America/New_York", True, policy)  # market YES=0.40
        self.assertIn("FADES_MARKET_FAVOURITE", d["reasons"])
        self.assertNotEqual(d["action"], "TRADE")

    def test_weight_one_reproduces_pure_model(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, model_weight=1.0,
                        min_net_edge=0.0, weather_fee_rate=0.0, slippage_per_share=0.0)
        outcome = {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}
        d = assess(STATE, outcome, "NO", QUOTE, AS_OF, "America/New_York", True, policy)
        self.assertAlmostEqual(d["net_edge"], 0.29)

    def test_locked_no_ignores_market_and_forecast_quality(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, model_weight=0.5)
        state = dict(STATE, components=[{"mean": 70, "calibration_days": 0,
                                         "fetched_at": "2026-09-01T00:00:00+00:00"}])
        quote = dict(QUOTE, best_bid=0.96, best_ask=0.97)
        outcome = {"market_id": "m", "p_yes": 0.0, "p_no": 1.0, "locked_no": True}
        d = assess(state, outcome, "NO", quote, AS_OF, "America/New_York", True, policy)
        self.assertTrue(d["locked_no"])
        self.assertEqual(d["action"], "TRADE", d["reasons"])


class PickerTest(unittest.TestCase):
    def test_picks_safest_not_lowest_ask(self):
        favourite = {"action": "TRADE", "side": "NO", "best_ask": 0.55, "blended_probability": 0.70, "net_edge": 0.12}
        tail = {"action": "TRADE", "side": "NO", "best_ask": 0.90, "blended_probability": 0.97, "net_edge": 0.06}
        self.assertIs(best_no_trade([favourite, tail]), tail)

    def test_locked_first(self):
        normal = {"action": "TRADE", "side": "NO", "best_ask": 0.8, "blended_probability": 0.99, "net_edge": 0.15}
        locked = {"action": "TRADE", "side": "NO", "best_ask": 0.97, "blended_probability": 1.0,
                  "net_edge": 0.03, "locked_no": True}
        self.assertIs(best_no_trade([normal, locked]), locked)


class EvaluateReportTest(unittest.TestCase):
    def test_report_runs_on_synthetic_records(self):
        from evaluate_model import market_probs, report
        mk = market_probs({"a": {"YES": {"best_bid": 0.6, "best_ask": 0.62}},
                           "b": {"NO": {"best_bid": 0.6, "best_ask": 0.62}}})
        self.assertAlmostEqual(sum(mk.values()), 1.0)
        records = []
        for e in range(4):
            for m, y in (("a", 1), ("b", 0)):
                records.append({"event_id": str(e), "city": "NYC", "target_date": "2026-09-25",
                                "checkpoint": "D 12:00", "as_of": "", "market_id": m, "yes_result": y,
                                "v3_yes": 0.3 if m == "a" else 0.7, "v4_yes": 0.8 if m == "a" else 0.2,
                                "market_yes": 0.6 if m == "a" else 0.4, "no_bid": 0.5, "no_ask": 0.55,
                                "locked_no": 0, "calibrated_models": 3})
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            report(records)
        text = buffer.getvalue()
        self.assertIn("ACCURACY", text)
        self.assertIn("STRATEGY SIM", text)


if __name__ == "__main__":
    unittest.main()


class LockedOnlyTest(unittest.TestCase):
    def test_locked_only_blocks_forecast_trades(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, min_net_edge=0.0,
                        max_fade_market_yes=1.0, locked_only=True)
        outcome = {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}
        d = assess(STATE, outcome, "NO", QUOTE, AS_OF, "America/New_York", True, policy)
        self.assertIn("LOCKED_ONLY_MODE", d["reasons"])
        self.assertNotEqual(d["action"], "TRADE")

    def test_locked_candidates_skip_markets_already_bought(self):
        from paper import PAPER_DDL, locked_candidates
        db = sqlite3.connect(":memory:")
        db.executescript(PAPER_DDL)
        db.execute("""INSERT INTO paper_orders(event_id,market_id,side,token_id,signal_at,execute_after,
                      status,model_probability,signal_ask,desired_shares,max_price)
                      VALUES ('e1','m1','NO','t','x','x','FILLED',1,0.97,10,0.99)""")
        decisions = [{"action": "TRADE", "side": "NO", "market_id": m, "locked_no": True} for m in ("m1", "m2")]
        self.assertEqual([d["market_id"] for d in locked_candidates(db, "e1", decisions)], ["m2"])


class UnitSafetyTest(unittest.TestCase):
    def test_implausible_observation_does_not_lock(self):
        # Forecast 21C (~70F) but readings say 90F: likely bad data, so no lock.
        db = build_db([(12, 90.0), (13, 90.5)], {h: 18.0 for h in range(18, 24)})
        state = evaluate(db, "e1", datetime(2026, 9, 25, 16, 0, tzinfo=NY).astimezone(timezone.utc), persist=False)
        self.assertFalse(any(p["locked_no"] for p in state["probabilities"]))


class DiscoveryTest(unittest.TestCase):
    def test_upcoming_slugs_for_known_cities(self):
        from datetime import date
        from collector import upcoming_slugs
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        db.execute("INSERT INTO events VALUES ('1','highest-temperature-in-nyc-on-september-28-2026','t','{}','x')")
        slugs = upcoming_slugs(db, 2, today=date(2026, 9, 30))
        self.assertIn("highest-temperature-in-nyc-on-october-2-2026", slugs)
        self.assertIn("highest-temperature-in-nyc-on-september-29-2026", slugs)
        self.assertNotIn("highest-temperature-in-nyc-on-september-28-2026", slugs)


class StationHistoryTest(unittest.TestCase):
    def _db(self):
        from datetime import date, timedelta as td
        from station_history import RESIDUAL_DDL
        db = sqlite3.connect(":memory:")
        db.executescript(RESIDUAL_DDL)
        start = date(2026, 6, 1)
        for i in range(120):
            day = (start + td(days=i)).isoformat()
            for model, resid in (("ecmwf_ifs025", 1.0 + (0.5 if i % 2 else -0.5)),
                                 ("gfs_global", -2.0 + (1.5 if i % 2 else -1.5))):
                db.execute("INSERT INTO station_residuals VALUES ('NYC','KLGA',?,1,?,20,21,?,'x')",
                           (model, day, resid))
        return db

    def test_fit_uses_only_past_days_and_scales_to_f(self):
        from probability import station_history_fit
        db = self._db()
        as_of = datetime(2026, 9, 1, 16, tzinfo=timezone.utc)
        fit = station_history_fit(db, "NYC", "F", 1, as_of, NY, 0.1)
        self.assertEqual(fit["ecmwf_ifs025"]["days"], 45)
        self.assertAlmostEqual(fit["ecmwf_ifs025"]["bias"], 1.8, places=1)   # 1.0C bias = 1.8F
        self.assertGreater(fit["gfs_global"]["sigma"], fit["ecmwf_ifs025"]["sigma"])
        early = station_history_fit(db, "NYC", "F", 1, datetime(2026, 6, 20, tzinfo=timezone.utc), NY, 0.1)
        self.assertEqual(early, {})  # fewer than 30 past days -> not used

    def test_sharper_model_gets_more_weight(self):
        from probability import component_weights
        w = component_weights([{"sigma": 1.0}, {"sigma": 2.0}])
        self.assertAlmostEqual(w[0], 0.8)

    def test_parsers(self):
        from unittest.mock import patch
        from datetime import date
        import station_history as sh
        csv_text = "#DEBUG\nstation,valid,tmpf\n" + "".join(
            f"LGA,2026-09-10 {h:02d}:51,{60 + h * 0.5}\n" for h in range(4, 24)) + "".join(
            f"LGA,2026-09-11 {h:02d}:51,75\n" for h in range(0, 4))
        with patch.object(sh, "http_get", return_value=csv_text.encode()):
            highs = sh.station_daily_highs("KLGA", date(2026, 9, 10), date(2026, 9, 10), NY)
        self.assertAlmostEqual(highs["2026-09-10"], (75 - 32) * 5 / 9, places=6)  # 00-03Z Sep 11 is still Sep 10 in NY
        payload = {"hourly": {"time": [f"2026-09-10T{h:02d}:00" for h in range(24)],
                              "temperature_2m": [20.0] * 24,
                              "temperature_2m_previous_day1": [21.0] * 23 + [25.0],
                              "temperature_2m_previous_day2": [None] * 24}}
        with patch.object(sh, "http_get", return_value=json.dumps(payload).encode()):
            fc = sh.forecast_daily_highs("gfs_global", 40, -73, "America/New_York", date(2026, 9, 10), date(2026, 9, 10))
        self.assertEqual(fc[1]["2026-09-10"], 25.0)
        self.assertEqual(fc[2], {})


class RulesAndUncertaintyTest(unittest.TestCase):
    def test_uncertain_forecast_not_traded(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, min_net_edge=0.0,
                        max_fade_market_yes=1.0)
        state = dict(STATE, components=[dict(c, sigma=4.5) for c in STATE["components"]])  # 4.5F = 2.5C
        d = assess(state, {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}, "NO", QUOTE, AS_OF,
                   "America/New_York", True, policy)
        self.assertIn("FORECAST_TOO_UNCERTAIN", d["reasons"])

    def test_weather_underground_rules_verify(self):
        from verify_rules import verify_event_payload
        desc = ("This market will resolve to the highest temperature recorded at the Taipei Songshan Airport "
                "Station in degrees Celsius on 1 Oct '26, as reported by Weather Underground at "
                "https://www.wunderground.com/history/daily/tw/taipei/RCSS. Resolves in whole degrees Celsius. "
                "Fahrenheit readings are not used.")
        markets = [{"id": "a", "question": "Will the highest temperature in Taipei be 29°C or below on October 1?"},
                   {"id": "b", "question": "Will the highest temperature in Taipei be 30°C on October 1?"},
                   {"id": "c", "question": "Will the highest temperature in Taipei be 31°C or higher on October 1?"}]
        event = {"id": "e", "title": "Highest temperature in Taipei on October 1?",
                 "slug": "highest-temperature-in-taipei-on-october-1-2026",
                 "markets": [dict(m, description=desc) for m in markets]}
        result = verify_event_payload(event)
        self.assertEqual((result["station"], result["unit"]), ("RCSS", "C"))


class BacktestEarlyTest(unittest.TestCase):
    def test_fit_never_uses_decision_day_or_later(self):
        from datetime import date, timedelta as td
        import backtest_early as bt
        series = {}
        for i in range(60):
            d = date(2026, 7, 1) + td(days=i)
            # huge error only on/after Aug 10 -> must not affect a fit decided on Aug 10
            err = 50.0 if d >= date(2026, 8, 10) else (1.0 if i % 2 else -1.0)
            series[d.isoformat()] = {"m": (20.0, 20.0 + err)}
        fit = bt.fit_components(series, date(2026, 8, 10), 90, "equal-mix", 1.0)
        self.assertLess(abs(fit["fits"]["m"][0]), 0.2)
        self.assertLess(fit["fits"]["m"][1], 1.2)


class CombinedFitTest(unittest.TestCase):
    def test_combined_uses_past_only_and_is_sharper(self):
        import random
        from datetime import date, timedelta as td
        from probability import combined_fit
        from station_history import RESIDUAL_DDL
        random.seed(3)
        db = sqlite3.connect(":memory:")
        db.executescript(RESIDUAL_DDL)
        for i in range(80):
            day = (date(2026, 7, 1) + td(days=i)).isoformat()
            actual = 25 + random.gauss(0, 3)
            for m, bias, sd in (("a", 2.0, 1.0), ("b", -1.0, 1.0), ("c", 0.0, 2.0)):
                f = actual - bias + random.gauss(0, sd)
                db.execute("INSERT INTO station_residuals VALUES ('X','S',?,1,?,?,?,?,'x')",
                           (m, day, f, actual, actual - f))
        fit = combined_fit(db, "X", "C", 1, datetime(2026, 9, 1, 12, tzinfo=timezone.utc),
                           ZoneInfo("UTC"), ["a", "b", "c"], 0.1)
        self.assertEqual(fit["days"], 45)
        self.assertAlmostEqual(fit["model_bias"]["a"], 2.0, delta=0.5)
        self.assertLess(fit["sigma"], 1.0)          # averaging beats any single model
        self.assertGreater(fit["weights"]["a"], fit["weights"]["c"])
        self.assertIsNone(combined_fit(db, "X", "C", 1, datetime(2026, 7, 10, tzinfo=timezone.utc),
                                       ZoneInfo("UTC"), ["a", "b", "c"], 0.1))


class LeadDaysTest(unittest.TestCase):
    def test_forecast_bets_only_two_days_ahead(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, min_net_edge=0.0,
                        max_fade_market_yes=1.0, min_forecast_lead_days=2)
        outcome = {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}
        far = assess(STATE, outcome, "NO", QUOTE, datetime(2026, 9, 25, 16, tzinfo=timezone.utc),
                     "America/New_York", True, policy)          # target 9/27 -> 2 days
        near = assess(STATE, outcome, "NO", QUOTE, datetime(2026, 9, 26, 16, tzinfo=timezone.utc),
                      "America/New_York", True, policy)         # 1 day
        self.assertEqual(far["action"], "TRADE", far["reasons"])
        self.assertIn("TOO_CLOSE_TO_TARGET_DAY", near["reasons"])


class VerifyStoreTest(unittest.TestCase):
    def test_verified_rules_overwrite_stale_auto_parse(self):
        from unittest.mock import patch
        import verify_rules
        desc = ("Highest temperature recorded at Taipei Songshan Airport, per Weather Underground "
                "https://www.wunderground.com/history/daily/tw/taipei/RCSS in whole degrees Celsius. "
                "Some sites show Fahrenheit.")
        markets = [{"id": "a", "question": "Will the highest temperature in Taipei be 29°C or below on October 3?"},
                   {"id": "b", "question": "Will the highest temperature in Taipei be 30°C on October 3?"},
                   {"id": "c", "question": "Will the highest temperature in Taipei be 31°C or higher on October 3?"}]
        event = {"id": "77", "title": "Highest temperature in Taipei on October 3?",
                 "slug": "highest-temperature-in-taipei-on-october-3-2026",
                 "markets": [dict(m, description=desc, outcomes='["Yes","No"]',
                                  clobTokenIds=f'["{m["id"]}y","{m["id"]}n"]') for m in markets]}
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        save_event(db, event)
        db.execute("UPDATE rule_reviews SET unit='F', station='TAIPEI'")  # stale bad auto-parse
        with patch.object(verify_rules, "get_json", return_value=event):
            result = verify_rules.verify_and_store(db, "77")
        self.assertEqual(result["verified_markets"], 3)
        self.assertEqual(set(db.execute("SELECT station,unit,verified FROM rule_reviews")), {("RCSS", "C", 1)})


class SigmaFloorTest(unittest.TestCase):
    def test_exact_limit_in_fahrenheit_is_not_blocked(self):
        policy = Policy(allowed_sides=("NO",), require_verified_rules=False, min_net_edge=0.0,
                        max_fade_market_yes=1.0)
        state = dict(STATE, combined={"sigma": 1.5 * 9 / 5})  # exactly 1.5C expressed in F
        d = assess(state, {"market_id": "m", "p_yes": 0.1, "p_no": 0.9}, "NO", QUOTE, AS_OF,
                   "America/New_York", True, policy)
        self.assertNotIn("FORECAST_TOO_UNCERTAIN", d["reasons"])


class RecentBookHistoryTest(unittest.TestCase):
    def test_recent_window_gives_same_quote_as_full_history(self):
        import json as _json
        from edge import book_history, quoted_book, recent_book_history
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        def add(kind, when, payload):
            db.execute("INSERT INTO book_events(token_id,event_type,source_timestamp,received_at,best_bid,best_ask,raw_json)"
                       " VALUES ('t',?,'',?,NULL,NULL,?)", (kind, when, _json.dumps(payload)))
        add("book", "2026-10-01T00:00:00+00:00", {"bids": [{"price": "0.10", "size": "5"}], "asks": [{"price": "0.90", "size": "5"}]})
        add("book", "2026-10-01T05:00:00+00:00", {"bids": [{"price": "0.40", "size": "5"}], "asks": [{"price": "0.45", "size": "7"}]})
        add("price_change", "2026-10-01T05:30:00+00:00",
            {"price_changes": [{"asset_id": "t", "price": "0.44", "size": "3", "side": "SELL"}]})
        as_of = datetime(2026, 10, 1, 6, tzinfo=timezone.utc)
        full = quoted_book(book_history(db, {"t"})["t"], as_of, 10 ** 6)
        recent = recent_book_history(db, {"t"}, as_of)
        self.assertEqual(len(recent["t"]), 2)  # only from the 05:00 snapshot onward
        self.assertEqual(quoted_book(recent["t"], as_of, 10 ** 6)["ask_levels"], full["ask_levels"])


class RecentStatsTest(unittest.TestCase):
    def test_since_fixes_split_by_days_ahead(self):
        from paper import PAPER_DDL, recent_stats
        db = sqlite3.connect(":memory:")
        db.executescript(DDL)
        db.executescript(PAPER_DDL)
        db.execute("INSERT INTO events VALUES ('e','s','t','{}','x')")
        db.execute("INSERT INTO markets VALUES ('m','e','s','q','d','c','NYC','2026-10-03',1,'{}','x')")
        db.execute("INSERT INTO locations VALUES ('NYC',40,-73,'America/New_York','t','{}')")
        def order(i, when, status, cost, pnl):
            db.execute("""INSERT INTO paper_orders(event_id,market_id,side,token_id,signal_at,execute_after,status,
                          model_probability,signal_ask,desired_shares,max_price,total_cost,pnl)
                          VALUES ('e','m','NO','t',?,?,?,0.9,0.8,10,0.83,?,?)""",
                       (when, when + str(i), status, cost, pnl))
        order(1, "2026-09-29T12:00:00+00:00", "SETTLED_LOSS", 10, -10)   # old: excluded
        order(2, "2026-10-01T16:00:00+00:00", "SETTLED_WIN", 8, 2)       # 2 days before
        order(3, "2026-10-01T17:00:00+00:00", "FILLED", 8, None)
        order(4, "2026-10-03T15:00:00+00:00", "SETTLED_WIN", 9.9, 0.1)   # same day
        out = recent_stats(db, "2026-10-01T00:00:00+00:00")
        two = out["2+ days before (forecast bets)"]
        self.assertEqual((two["bets_placed"], two["settled"], two["wins"], two["waiting_to_settle"]), (2, 1, 1, 1))
        self.assertEqual(two["return_pct"], 25.0)
        self.assertEqual(out["same day (locked trades)"]["wins"], 1)


class NwsTest(unittest.TestCase):
    def test_parse_and_store_without_touching_live_model(self):
        import asyncio
        from unittest.mock import patch
        import collector
        from probability import latest_forecasts
        grid = {"properties": {"updateTime": "2026-10-04T10:00:00+00:00", "maxTemperature": {"values": [
            {"validTime": "2026-10-05T11:00:00+00:00/PT13H", "value": 22.2},
            {"validTime": "2026-10-06T11:00:00+00:00/PT13H", "value": 24.4}]}}}
        self.assertEqual(collector.nws_daily_highs(grid, "America/New_York"),
                         {"2026-10-05": 22.2, "2026-10-06": 24.4})
        db = build_db([(12, 70.0)], {})
        async def fake(url):
            return {"properties": {"forecastGridData": "https://api.weather.gov/gridpoints/OKX/1,1"}} if "/points/" in url else grid
        with patch.object(collector, "fetch", side_effect=fake):
            asyncio.run(collector.collect_nws(db, "NYC", 40.77, -73.88, "America/New_York"))
        self.assertEqual(db.execute("SELECT COUNT(*) FROM forecast_values WHERE period='nws_daily_max'").fetchone()[0], 2)
        live = latest_forecasts(db, "NYC", "2026-09-25", datetime(2026, 10, 9, tzinfo=timezone.utc))
        self.assertNotIn("nws", live)


class TelegramTest(unittest.TestCase):
    def test_message_sent_and_failures_never_crash(self):
        import os
        from unittest.mock import patch
        import live
        self.assertEqual(live.short_question("Will the highest temperature in NYC be between 68-69°F on October 5?"),
                         "NYC: between 68-69°F")
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "123:abc", "TELEGRAM_CHAT_ID": "42"}):
            with patch.object(live, "urlopen") as fake:
                live.telegram("hi")
                request = fake.call_args[0][0]
                self.assertIn("api.telegram.org/bot123:abc/sendMessage", request.full_url)
            with patch.object(live, "urlopen", side_effect=OSError("down")):
                live.telegram("hi")  # must not raise
        with patch.dict(os.environ, {}, clear=True), patch.object(live, "urlopen") as fake:
            live.telegram("hi")
            fake.assert_not_called()
