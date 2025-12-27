# PWHL Scraper and Visualization

This project scrapes data from the Professional Women's Hockey League (PWHL) API and creates visualizations of the data.

## Features
- Schedule scraping
- Data visualization
- Multiple seasons support (2023/2024 and 2024/2025)

## Season Mapping
- Season 1: 2023/2024 Regular Season
- Season 3: 2023/2024 Playoffs  
- Season 5: 2024/2025 Regular Season
- Season 6: 2024/2025 Playoffs

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Run the scraper: `python scraper.py`

## Web App
Run the Flask app locally:
- `C:/Users/larss/Apps/PWHL/.venv/Scripts/python.exe flask_app.py`

## Buy Me a Coffee (Stripe)
The web app includes a `/coffee` page that uses Stripe Checkout.

Required env var:
- `STRIPE_SECRET_KEY`

Optional env vars:
- `STRIPE_CURRENCY` (default: `usd`)
- `STRIPE_PRODUCT_NAME` (default: `PWHL Analytics - Coffee`)

## Usage
The scraper will fetch schedule data from the PWHL API and save it as JSON files for further analysis.