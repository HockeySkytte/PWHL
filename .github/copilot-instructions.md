# PWHL Analytics – Copilot Instructions

## Project overview

A Flask web app + data pipeline for Professional Women's Hockey League analytics. It scrapes the HockeyTech API, serves schedule/game/report data via a REST API, renders HTML pages with vanilla JS, and bulk-exports CSV datasets for offline analysis and an xG model.

## Architecture

- **`flask_app.py`** (~2700 lines) – The monolith. Contains `PWHLDataAPI` (HockeyTech client), all `/api/*` routes, template rendering, Stripe checkout, and the `data_api` singleton. Runs on `localhost:8501`.
- **`export_utils.py`** – Pure-function CSV generators (`generate_lineups_csv`, `generate_pbp_csv`). Imported by both `flask_app.py` (live export endpoints) and `scripts/export_all_csvs.py` (bulk CLI). Changes here affect both paths.
- **`report_data.py`** – `ReportDataStore` class that reads from Supabase for the Report page aggregation API (`/api/report/*`).
- **`scripts/export_all_csvs.py`** – CLI bulk exporter. Calls the local Flask API _or_ falls back to direct HockeyTech feed, then upserts data to Supabase by default. CSV writing is optional.
- **`build_xg_model.py`** – Trains an xG logistic regression from the exported PBP CSVs; outputs to `models/`.
- **`templates/`** – Jinja2 HTML pages; heavy client-side JS in `templates/game.html` and `templates/report/`.

## Data flow

```
HockeyTech API ──► PWHLDataAPI (flask_app.py) ──► /api/* JSON endpoints
                                                      │
                        scripts/export_all_csvs.py ◄───┘
                                │
                             Supabase
                                │
                    report_data.py (ReportDataStore) ──► /api/report/* endpoints
                    build_xg_model.py ──► models/xg_model.json
```

## Key conventions

- **HockeyTech API quirk**: Responses are JSONP — JSON wrapped in parentheses `(...)`. All fetch methods strip the wrapping before `json.loads()`.
- **Seasons**: Season metadata should come from the live `modulekit/seasons` endpoint. Only use hardcoded season IDs as an offline fallback.
- **Team identity**: `Teams.csv` is the single source of truth for team names, IDs, colors, and logos. Team names must match this file exactly (watch for `Montréal` accented é). Use `city_to_full_name` mapping for city→full-name resolution.
- **CSV column naming**: PBP exports use snake_case headers (`game_id`, `team_home`, `p1_name`, `xG`, `strength`). Lineup exports use Title Case (`Number`, `Name`, `Team`, `TOI`).
- **Strength strings**: Formatted as `"XvY"` (e.g. `"5v5"`, `"5v4"`, `"4v5"`, `"ENA"` for empty-net). Generated in `generate_pbp_csv` via on-ice skater counting logic.
- **Coordinate system**: Raw HockeyTech coords are 600×300 canvas pixels. Converted to feet via `(x-300)/300*100` and `(y-150)/150*42.5` in `export_utils.convert_x`/`convert_y`.

## Developer workflow

```powershell
# Activate venv (Windows)
& .venv/Scripts/Activate.ps1

# Run the Flask app (must be running for export and tests)
python flask_app.py          # serves on http://localhost:8501

# Export CSVs for a single game (requires flask running OR falls back to direct API)
python scripts/export_all_csvs.py --game-id 105

# Export all games (optionally filter by date range)
python export_all_csvs.py --start-date 2025-01-01 --end-date 2025-01-31

# Run tests (pytest; excludes PWHL-b*, Learning/, scripts/ dirs)
pytest
```

- **`PWHL_BASE_URL`** env var overrides the default `http://localhost:8501` for export scripts and CI.
- **`STRIPE_SECRET_KEY`** env var required for the `/coffee` payment page.
- The GitHub Actions workflow (`.github/workflows/daily_export.yml`) runs hourly but gates to noon Europe/Copenhagen, exporting yesterday's games and upserting them to Supabase in `--no-csv` mode.

## Testing

- Tests in `test_*.py` at the project root; `conftest.py` provides a `game_id` fixture (default `105`, overridable via `PWHL_TEST_GAME_ID`).
- Tests are integration-style: they hit `localhost:8501` endpoints. **The Flask app must be running** for API tests to pass.
- `pytest.ini` excludes `PWHL-b*`, `Learning/`, and `scripts/` from test collection.

## Pitfalls to avoid

- `export_utils.py` is shared between the Flask server and the CLI exporter. Test both paths after changes.
