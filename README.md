# Polymarket weather collector (step 1)

This is a **read-only** collector. It records event and market rules, YES/NO token mappings, CLOB WebSocket messages and Open-Meteo model forecasts in SQLite. It does not generate signals or place orders.

## Run

Python 3.11+:

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -m collector collect --config config.json
```

To make a short live recording: `python -m collector collect --config config.json --seconds 60`. `--once` fetches metadata and forecasts but does not open the order book stream.

The collector uses Polymarket Gamma `/public-search` and `/events/{id}` (or configured `/events/slug/{slug}`), CLOB's **public** market WebSocket, Open-Meteo's geocoding and forecast APIs. No wallet, trading client, key, or database server is required. For an event missing from public search, add its exact event slug to `event_slugs`. If automatic geocoding chooses the wrong place, set `city_locations` to `{"London": {"latitude": 51.5, "longitude": -0.12, "timezone": "Europe/London"}}`. This is the **forecast location**, not proof of the resolution station.

Inspect the SQLite database with `python -m collector status --config config.json`. Every market's source description and original response are retained. `rule_reviews` lists markets requiring human review of the precise station, units, rounding, date, and measurement window. **Do not treat unresolved rules as validated outcomes.** Forecast values from Open-Meteo's live API are stitched feeds; `model_run_at` is NULL, while `fetched_at` and the complete response are saved. For exact model run attribution, an explicit single-runs collector will be needed.

Official observed temperatures differ by market resolution source. After verifying the source and station, import the authoritative records using a CSV with columns `market_id,station,observed_at,temperature,unit,source_url` and optional `source_record_id`. Example:

```bash
python -m collector import-observations --config config.json --csv verified_observations.csv
```

The import retains the source URL and ingestion time; it does not scrape or substitute modelled weather as a station observation. Data gaps during WebSocket disconnects remain gaps. Database timestamps are UTC; `forecast_values.valid_time` includes the location's offset.

## Tables

`events`, `markets`, `tokens`, `rule_reviews`, `locations`, `forecast_batches`, `forecast_values`, `book_events`, `observations`. `book_events` keeps the raw CLOB frame, source timestamp and receive timestamp; full `book` messages also contain best prices and full depth in JSON. `forecast_batches` stores original API replies; per-hour and per-day rows live in `forecast_values`. Historical records are append-only; metadata can be refreshed. Daily max records have a location-local date; hourly records include the location's UTC offset.

## API references

- https://docs.polymarket.com/market-data/discover-markets
- https://docs.polymarket.com/market-data/realtime-data
- https://open-meteo.com/en/docs
- https://open-meteo.com/en/docs/single-runs-api
