# PWHL Analytics

Flask-based PWHL analytics app with a Supabase-backed data store, HockeyTech schedule/game ingestion, and report/game/skater/goalie views.

## Core commands

1. Install dependencies: `pip install -r requirements.txt`
2. Run the app: `python flask_app.py`
3. Export/upsert games: `python scripts/export_all_csvs.py --start-date 2026-04-28 --end-date 2026-05-28`

## Seasons

Season metadata is fetched live from the HockeyTech `modulekit/seasons` endpoint. Regular season and playoff IDs are no longer hardcoded in the exporter path, so newly published seasons like 2025/2026 Playoffs are picked up automatically.

## Environment

- `PWHL_BASE_URL` overrides the default local Flask base URL for export scripts.
- `SUPABASE_URL` and `SUPABASE_KEY` enable Supabase upserts.
- `STRIPE_SECRET_KEY` enables the `/coffee` checkout flow.

## Notes

- The production app is `flask_app.py`.
- The bulk exporter lives in `scripts/export_all_csvs.py`.
- The scheduled GitHub workflow runs the exporter in Supabase-only mode; it does not commit CSV files back to the repo.