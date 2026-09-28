import unittest

from verify_rules import verify_event_payload


def valid_event():
    rules = (
        'This market will resolve to the temperature range that contains the highest temperature '
        'recorded by NOAA at the LaGuardia Airport Station in degrees Fahrenheit on 28 Sep \'26. '
        'The resolution source for this market will be information from NOAA, specifically the '
        'highest reading under the "Temp" column for all times on this day, available here: '
        'https://www.weather.gov/wrh/timeseries?site=KLGA '
        'This market will resolve off of the Hourly Data provided using the "Show Hourly Data" button. '
        'If NOAA data for the observation date is unavailable by 11:59 PM ET on the day following '
        'the observation date, the Weather Underground Daily Observations table will be used as the '
        'resolution source. This market will resolve once the first data point for the following date '
        'has been published on the resolution source. The resolution source for this market measures '
        'temperatures to whole degrees Fahrenheit.'
    )
    questions = [
        ("1", "Will the highest temperature in New York City be 59°F or below on September 28?"),
        ("2", "Will the highest temperature in New York City be between 60-61°F on September 28?"),
        ("3", "Will the highest temperature in New York City be 62°F or higher on September 28?"),
    ]
    return {
        "id": "1084295",
        "title": "Highest temperature in NYC on September 28?",
        "slug": "highest-temperature-in-nyc-on-september-28-2026",
        "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=KLGA",
        "description": rules,
        "markets": [
            {
                "id": market_id,
                "question": question,
                "description": rules,
                "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=KLGA",
            }
            for market_id, question in questions
        ],
    }


class VerifyRulesTest(unittest.TestCase):
    def test_accepts_current_noaa_weather_rule_shape(self):
        result = verify_event_payload(valid_event())
        self.assertEqual(result["station"], "KLGA")
        self.assertEqual(result["unit"], "F")
        self.assertEqual(result["rounding"], "whole degree")
        self.assertEqual(result["market_count"], 3)


    def test_accepts_celsius_noaa_rule_shape(self):
        event = valid_event()
        event["title"] = "Highest temperature in Tokyo on September 28?"
        event["slug"] = "highest-temperature-in-tokyo-on-september-28-2026"
        for market in event["markets"]:
            market["description"] = (
                market["description"]
                .replace("LaGuardia Airport", "Tokyo Haneda Airport")
                .replace("Fahrenheit", "Celsius")
                .replace("site=KLGA", "site=RJTT")
            )
            market["resolutionSource"] = "https://www.weather.gov/wrh/timeseries?site=RJTT"
        event["markets"][0]["question"] = "Will the highest temperature in Tokyo be 18°C or below on September 28?"
        event["markets"][1]["question"] = "Will the highest temperature in Tokyo be between 19-20°C on September 28?"
        event["markets"][2]["question"] = "Will the highest temperature in Tokyo be 21°C or higher on September 28?"
        result = verify_event_payload(event)
        self.assertEqual(result["station"], "RJTT")
        self.assertEqual(result["unit"], "C")
        self.assertEqual(result["rounding"], "whole degree")

    def test_rejects_missing_hourly_data_clause(self):
        event = valid_event()
        for market in event["markets"]:
            market["description"] = market["description"].replace("Hourly Data", "data")
        with self.assertRaisesRegex(ValueError, "hourly_data"):
            verify_event_payload(event)

    def test_rejects_station_source_mismatch(self):
        event = valid_event()
        event["markets"][1]["resolutionSource"] = "https://www.weather.gov/wrh/timeseries?site=KJFK"
        event["markets"][1]["description"] = event["markets"][1]["description"].replace(
            "site=KLGA", "site=KJFK"
        )
        with self.assertRaisesRegex(ValueError, "identical resolution rule set"):
            verify_event_payload(event)


if __name__ == "__main__":
    unittest.main()
