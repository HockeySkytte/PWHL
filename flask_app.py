
from flask import Flask, render_template, jsonify, request, send_from_directory, Response, url_for
from flask_cors import CORS
import requests
import json
import pandas as pd
from datetime import datetime
import csv
import os
from typing import Dict, Any, List

try:
    import stripe  # type: ignore
except Exception:
    stripe = None

app = Flask(__name__)
CORS(app)


def _team_logo_url(team_name: str) -> str:
    """Resolve a team's logo URL from Teams.csv-loaded metadata.

    Supports both full team names (e.g., 'Boston Fleet') and common
    city-name aliases (e.g., 'Boston') via data_api.city_to_full_name.
    """
    try:
        if not team_name:
            return ''
        teams = getattr(data_api, 'teams', {}) or {}
        if team_name in teams:
            return (teams.get(team_name, {}) or {}).get('logo', '') or ''
        city_map = getattr(data_api, 'city_to_full_name', {}) or {}
        full = city_map.get(team_name)
        if full and full in teams:
            return (teams.get(full, {}) or {}).get('logo', '') or ''
        return ''
    except Exception:
        return ''


_SCHEDULE_GAMES_CACHE: Dict[int, List[Dict[str, Any]]] = {}


def _get_parsed_schedule_games(season_id: int) -> List[Dict[str, Any]]:
    """Return parsed schedule games for a given API season id (cached)."""
    try:
        sid = int(season_id)
    except Exception:
        return []
    if sid in _SCHEDULE_GAMES_CACHE:
        return _SCHEDULE_GAMES_CACHE[sid]
    try:
        raw = data_api.fetch_schedule_data(sid)
        games = data_api.parse_games_data(raw, sid) or []
        _SCHEDULE_GAMES_CACHE[sid] = games
        return games
    except Exception:
        _SCHEDULE_GAMES_CACHE[sid] = []
        return []


def _season_ids_for_filters(season_year: str, season_state: str) -> List[int]:
    """Map UI season/year + season_state to HockeyTech season ids."""
    ids: List[int] = []
    mapping = getattr(data_api, 'season_mapping', {}) or {}

    if season_year == 'All' and season_state == 'All':
        return [int(s) for s in (getattr(data_api, 'all_seasons', []) or []) if str(s).isdigit()]

    if season_year != 'All' and season_state != 'All':
        sid = (mapping.get(season_year, {}) or {}).get(season_state)
        if sid is not None:
            ids.append(int(sid))
        return ids

    if season_year != 'All' and season_state == 'All':
        for sid in (mapping.get(season_year, {}) or {}).values():
            if sid is not None:
                ids.append(int(sid))
        return ids

    # season_year == 'All' and season_state != 'All'
    for yr, states in mapping.items():
        sid = (states or {}).get(season_state)
        if sid is not None:
            ids.append(int(sid))
    return ids


def _compute_team_points_table(season_year: str, season_state: str) -> Dict[str, Dict[str, Any]]:
    """Compute standings-style points (3-2-1-0) from schedule results."""
    results: Dict[str, Dict[str, Any]] = {}
    season_ids = _season_ids_for_filters(season_year, season_state)
    for sid in season_ids:
        for g in _get_parsed_schedule_games(sid):
            status = (g.get('status') or '').strip()
            if not status.startswith('Final'):
                continue
            home = (g.get('home_team') or '').strip()
            away = (g.get('away_team') or '').strip()
            if not home or not away:
                continue
            try:
                hs = int(g.get('home_score') or 0)
                as_ = int(g.get('away_score') or 0)
            except Exception:
                continue
            if hs == as_:
                continue

            is_otso = ('OT' in status) or ('SO' in status)
            winner = home if hs > as_ else away
            loser = away if winner == home else home

            for t in (home, away):
                results.setdefault(t, {'GP': 0, 'W': 0, 'OTW': 0, 'OTL': 0, 'L': 0, 'Points': 0})
                results[t]['GP'] += 1

            if is_otso:
                results[winner]['OTW'] += 1
                results[winner]['Points'] += 2
                results[loser]['OTL'] += 1
                results[loser]['Points'] += 1
            else:
                results[winner]['W'] += 1
                results[winner]['Points'] += 3
                results[loser]['L'] += 1

    # Add Pct
    for t, rec in results.items():
        gp = int(rec.get('GP') or 0)
        pts = int(rec.get('Points') or 0)
        rec['Pct'] = (pts / (gp * 3)) if gp > 0 else None
    return results

# Allow embedding the app inside hockey-statistics.com/pwhl via iframe by setting
# a permissive frame-ancestors policy for that domain and removing X-Frame-Options.
# This keeps the app secure while enabling the desired WordPress integration.
@app.after_request
def _allow_wp_embed(resp: Response):
    try:
        # Allow both bare and www hostnames for WordPress page embedding
        allowed = "frame-ancestors 'self' https://hockey-statistics.com https://www.hockey-statistics.com"
        existing = resp.headers.get('Content-Security-Policy')
        if existing and 'frame-ancestors' in existing:
            # Respect existing CSP if it already defines frame-ancestors
            pass
        elif existing:
            resp.headers['Content-Security-Policy'] = f"{existing}; {allowed}"
        else:
            resp.headers['Content-Security-Policy'] = allowed
        # Remove X-Frame-Options to avoid blocking cross-origin iframes
        try:
            del resp.headers['X-Frame-Options']
        except Exception:
            pass
    except Exception:
        pass
    return resp

class PWHLDataAPI:
    def __init__(self):
        self.api_base_url = "https://lscluster.hockeytech.com/feed/index.php"
        self.api_key = "446521baf8c38984"
        self.client_code = "pwhl"
        
        # Season mapping - organized by year and type
        self.season_mapping = {
            "2023/2024": {
                "Regular Season": 1,
                "Playoffs": 3
            },
            "2024/2025": {
                "Regular Season": 5,
                "Playoffs": 6
            },
            "2025/2026": {
                "Regular Season": 8
            }
        }
        
        # All available seasons for API calls
        self.all_seasons = [1, 3, 5, 6, 8]  # Include new 2025/2026 Regular Season (8)
        
        # Season years
        self.season_years = ["2023/2024", "2024/2025", "2025/2026"]
        self.season_states = ["Regular Season", "Playoffs"]
        
        # Load team data
        self.teams = self.load_team_data()
    
    def load_team_data(self):
        """Load team data from Teams.csv"""
        teams = {}
        city_to_full_name = {}
        csv_path = os.path.join(os.path.dirname(__file__), 'Teams.csv')
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if not row or 'name' not in row:
                        continue
                    
                    team_data = {
                        'id': row.get('id', ''),
                        'name': row.get('name', ''),
                        'nickname': row.get('nickname', ''),
                        'team_code': row.get('team_code', ''),
                        'logo': row.get('logo', ''),
                        'color': row.get('color', '')
                    }
                    
                    # Use full team name as key for easy lookup
                    teams[row['name']] = team_data
                    
                    # Create city to full name mapping
                    # Handle special cases like "New York" and Montreal variations
                    if row['name'].startswith('New York'):
                        city_name = 'New York'
                    elif row['name'].startswith('Montréal'):
                        # Handle both Montreal and Montréal variations
                        city_to_full_name['Montreal'] = row['name']
                        city_to_full_name['Montréal'] = row['name']
                        city_name = 'Montréal'
                    else:
                        # Extract city (first or second word for 'PWHL City' style names)
                        parts = row['name'].split(' ')
                        if parts[0] == 'PWHL' and len(parts) > 1:
                            city_name = parts[1]  # e.g., 'Seattle' from 'PWHL Seattle'
                        else:
                            city_name = parts[0]
                    city_to_full_name[city_name] = row['name']
                    
            print(f"Loaded {len(teams)} teams: {list(teams.keys())}")
            print(f"City mapping: {city_to_full_name}")
        except Exception as e:
            print(f"Error loading team data: {e}")
            
        # Store the city mapping for later use
        self.city_to_full_name = city_to_full_name
        return teams
    
    def fetch_schedule_data(self, season):
        """Fetch schedule data from PWHL API"""
        params = {
            'feed': 'statviewfeed',
            'view': 'schedule',
            'team': -1,
            'season': season,
            'month': -1,
            'location': 'homeaway',
            'key': self.api_key,
            'client_code': self.client_code,
            'site_id': 0,
            'league_id': 1,
            'conference_id': -1,
            'division_id': -1,
            'lang': 'en'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(self.api_base_url, params=params, headers=headers)
            response.raise_for_status()
            
            # Clean the response (remove parentheses)
            raw_data = response.text.strip()
            if raw_data.startswith('(') and raw_data.endswith(')'):
                raw_data = raw_data[1:-1]
            
            data = json.loads(raw_data)
            
            # Extract games from the API response structure
            if isinstance(data, list) and len(data) > 0 and 'sections' in data[0]:
                sections = data[0]['sections']
                if sections and len(sections) > 0 and 'data' in sections[0]:
                    return sections[0]['data']
            return []
            
        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            return []
    
    def parse_games_data(self, games_data, season):
        """Parse games data into structured format"""
        parsed_games = []
        
        for game in games_data:
            row = game.get('row', {})
            
            # Determine season state and year based on season ID
            # Determine season state and year based on season ID
            if season in [1, 5, 8]:
                season_state = "Regular Season"
            elif season in [3, 6]:
                season_state = "Playoffs"
            else:
                season_state = "Regular Season"

            if season in [1, 3]:
                season_year = "2023/2024"
            elif season in [5, 6]:
                season_year = "2024/2025"
            elif season in [8]:
                season_year = "2025/2026"
            else:
                season_year = "Unknown"
            
            # Parse date with better year detection
            date_str = row.get('date_with_day', '')
            try:
                if date_str:
                    # Determine the correct year based on season
                    if season_year == "2023/2024":
                        # Games could be in 2023 or 2024
                        year = 2024 if "Jan" in date_str or "Feb" in date_str or "Mar" in date_str or "Apr" in date_str or "May" in date_str else 2023
                    elif season_year == "2024/2025":
                        # Games could be in 2024 or 2025
                        year = 2025 if "Jan" in date_str or "Feb" in date_str or "Mar" in date_str or "Apr" in date_str or "May" in date_str else 2024
                    elif season_year == "2025/2026":
                        # Games could be in 2025 or 2026
                        year = 2026 if "Jan" in date_str or "Feb" in date_str or "Mar" in date_str or "Apr" in date_str or "May" in date_str else 2025
                    else:
                        year = datetime.now().year
                    
                    date_parsed = pd.to_datetime(f"{date_str}, {year}", format='%a, %b %d, %Y', errors='coerce')
                    formatted_date = date_parsed.strftime('%a, %b %d') if pd.notna(date_parsed) else 'TBD'
                    full_date = date_parsed.strftime('%Y-%m-%d') if pd.notna(date_parsed) else ''
                else:
                    formatted_date = 'TBD'
                    full_date = ''
                    date_parsed = pd.NaT
            except:
                formatted_date = 'TBD'
                full_date = ''
                date_parsed = pd.NaT
            
            # Get team names from API (these are city names)
            away_team_city = row.get('visiting_team_city', '')
            home_team_city = row.get('home_team_city', '')
            
            # Convert city names to full team names and get logos
            away_team_full_name = self.city_to_full_name.get(away_team_city, away_team_city)
            home_team_full_name = self.city_to_full_name.get(home_team_city, home_team_city)
            
            away_team_logo = ''
            home_team_logo = ''
            if away_team_full_name in self.teams:
                away_team_logo = self.teams[away_team_full_name]['logo']
            if home_team_full_name in self.teams:
                home_team_logo = self.teams[home_team_full_name]['logo']
            
            # Resolve team IDs: prefer API fields; fallback to lookup by full team name from loaded Teams.csv
            away_team_id_val = row.get('visiting_team_id', '')
            home_team_id_val = row.get('home_team_id', '')
            if not away_team_id_val and away_team_full_name in self.teams:
                away_team_id_val = str(self.teams[away_team_full_name].get('id') or '')
            if not home_team_id_val and home_team_full_name in self.teams:
                home_team_id_val = str(self.teams[home_team_full_name].get('id') or '')

            game_info = {
                'date': formatted_date,
                'full_date': full_date,
                'date_obj': date_parsed,
                'season_year': season_year,
                'season_state': season_state,
                'away_team': away_team_full_name,  # Use full team name
                'home_team': home_team_full_name,  # Use full team name
                'away_team_id': away_team_id_val,
                'home_team_id': home_team_id_val,
                'away_team_city': away_team_city,  # Keep city for reference
                'home_team_city': home_team_city,  # Keep city for reference
                'away_team_logo': away_team_logo,
                'home_team_logo': home_team_logo,
                'status': row.get('game_status', ''),
                'away_score': row.get('visiting_goal_count', ''),
                'home_score': row.get('home_goal_count', ''),
                'game_id': row.get('game_id', ''),
                'venue': row.get('venue_name', '')
            }
            parsed_games.append(game_info)
        
        return parsed_games
    
    def fetch_game_summary(self, game_id):
        """Fetch game summary/lineup data from PWHL API"""
        params = {
            'feed': 'statviewfeed',
            'view': 'gameSummary',
            'game_id': game_id,
            'key': self.api_key,
            'site_id': 0,
            'client_code': self.client_code,
            'lang': 'en',
            'league_id': ''
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(self.api_base_url, params=params, headers=headers)
            response.raise_for_status()
            
            # Clean the response (remove parentheses)
            raw_data = response.text.strip()
            if raw_data.startswith('(') and raw_data.endswith(')'):
                raw_data = raw_data[1:-1]
            
            data = json.loads(raw_data)
            
            # Process and expand the nested team data
            processed_data = self.process_game_summary_data(data)
            return processed_data
            
        except Exception as e:
            print(f"Error fetching game summary for game {game_id}: {str(e)}")
            return None
    
    def process_game_summary_data(self, data):
        """Process and expand the nested game summary data"""
        if not data or not isinstance(data, dict):
            return data
            
        processed = {}
        
        # Process homeTeam data
        if 'homeTeam' in data and isinstance(data['homeTeam'], dict):
            home_team = data['homeTeam'].copy()
            processed['homeTeam'] = {
                'name': home_team.get('name', 'Home Team'),
                'goalies': [],
                'skaters': []
            }
            
            # Expand homeTeam goalies
            if 'goalies' in home_team and isinstance(home_team['goalies'], list):
                for goalie in home_team['goalies']:
                    if isinstance(goalie, dict):
                        expanded_goalie = self.expand_player_data(goalie)
                        if expanded_goalie:
                            processed['homeTeam']['goalies'].append(expanded_goalie)
            
            # Expand homeTeam skaters
            if 'skaters' in home_team and isinstance(home_team['skaters'], list):
                for skater in home_team['skaters']:
                    if isinstance(skater, dict):
                        expanded_skater = self.expand_player_data(skater)
                        if expanded_skater:
                            processed['homeTeam']['skaters'].append(expanded_skater)
        
        # Process visitingTeam data
        if 'visitingTeam' in data and isinstance(data['visitingTeam'], dict):
            visiting_team = data['visitingTeam'].copy()
            processed['visitingTeam'] = {
                'name': visiting_team.get('name', 'Visiting Team'),
                'goalies': [],
                'skaters': []
            }
            
            # Expand visitingTeam goalies
            if 'goalies' in visiting_team and isinstance(visiting_team['goalies'], list):
                for goalie in visiting_team['goalies']:
                    if isinstance(goalie, dict):
                        expanded_goalie = self.expand_player_data(goalie)
                        if expanded_goalie:
                            processed['visitingTeam']['goalies'].append(expanded_goalie)
            
            # Expand visitingTeam skaters
            if 'skaters' in visiting_team and isinstance(visiting_team['skaters'], list):
                for skater in visiting_team['skaters']:
                    if isinstance(skater, dict):
                        expanded_skater = self.expand_player_data(skater)
                        if expanded_skater:
                            processed['visitingTeam']['skaters'].append(expanded_skater)
        
        return processed
    
    def expand_player_data(self, player):
        """Expand player data by combining info and stats"""
        if not isinstance(player, dict):
            return None
            
        expanded = {}
        
        # Get info data
        if 'info' in player and isinstance(player['info'], dict):
            info = player['info']
            expanded.update({
                'name': f"{info.get('firstName', '')} {info.get('lastName', '')}".strip(),
                'jersey': info.get('jerseyNumber', ''),
                'position': info.get('position', ''),
                'birthDate': info.get('birthDate', ''),
                'playerImageURL': info.get('playerImageURL', '')
            })
        
        # Get stats data
        if 'stats' in player and isinstance(player['stats'], dict):
            stats = player['stats']
            expanded['stats'] = stats
        
        return expanded if expanded else None
    
    def fetch_play_by_play(self, game_id):
        """Fetch play-by-play data from PWHL API"""
        params = {
            'feed': 'statviewfeed',
            'view': 'gameCenterPlayByPlay',
            'game_id': game_id,
            'key': self.api_key,
            'site_id': 0,
            'client_code': self.client_code,
            'lang': 'en',
            'league_id': ''
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(self.api_base_url, params=params, headers=headers)
            response.raise_for_status()
            
            # Clean the response (remove parentheses)
            raw_data = response.text.strip()
            if raw_data.startswith('(') and raw_data.endswith(')'):
                raw_data = raw_data[1:-1]
            
            data = json.loads(raw_data)
            return data
            
        except Exception as e:
            print(f"Error fetching play-by-play for game {game_id}: {str(e)}")
            return None

# Initialize the data API
data_api = PWHLDataAPI()
from export_utils import generate_lineups_csv, generate_pbp_csv
from report_data import report_store

# Video events endpoint (moved here to ensure report_store is defined)
@app.route('/api/report/video_events')
def report_video_events():
    """Return all video-tagged events for the Video tab.

    Optional query params: team, season, season_state, games (comma or repeated), reload=1.
    """
    # Initial (lazy) load
    report_store.load()
    # Allow explicit reload or attempt forced if empty
    if request.args.get('reload') == '1' or not getattr(report_store, 'video_events', []):
        report_store.load(force=True)
    team = request.args.get('team', 'All')
    season = request.args.get('season', 'All')
    season_state = request.args.get('season_state', 'All')
    date_from = request.args.get('date_from','')
    date_to = request.args.get('date_to','')
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals=[v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    games = _get_multi('games') or None
    periods = _get_multi('periods') or None
    events_f = _get_multi('events') or None
    strengths = _get_multi('strengths') or None
    players = _get_multi('players') or None
    opponents = _get_multi('opponents') or None
    events = report_store.video_events_list(
        team=team, season=season, season_state=season_state, games=games,
        periods=periods, events=events_f, strengths=strengths, players=players,
        opponents=opponents, date_from=date_from, date_to=date_to
    )
    return jsonify({'events': events, 'count': len(events)})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/report')
def report_page():
    """Prototype report dashboard (UI only)."""
    return render_template('report.html')

@app.route('/data')
def data_page():
    """Data export page: filter and export Play-by-Play from CSVs."""
    return render_template('data.html')

@app.route('/skaters')
def skaters_page():
    """Skaters page: single-player view with filters and heat map."""
    return render_template('skaters.html')

@app.route('/goalies')
def goalies_page():
    """Goalies page: single-goalie view with filters and heat map."""
    return render_template('goalies.html')

@app.route('/teams')
def teams_page():
    """Teams page: team-level view with filters, standings, charts, and performance."""
    return render_template('teams.html')


@app.route('/coffee')
def coffee_page():
    """Buy me a coffee page (Stripe Checkout)."""
    success = request.args.get('success') == '1'
    canceled = request.args.get('canceled') == '1'
    stripe_enabled = (stripe is not None) and bool(os.environ.get('STRIPE_SECRET_KEY'))
    return render_template('coffee.html', success=success, canceled=canceled, stripe_enabled=stripe_enabled)


@app.route('/api/stripe/create-checkout-session', methods=['POST'])
def stripe_create_checkout_session():
    """Create a Stripe Checkout Session for a one-time 'coffee' payment.

    Expects JSON: {"amount": 3|5|10}
    Returns: {"url": "https://checkout.stripe.com/..."}
    """
    if stripe is None:
        return jsonify({'error': 'Stripe is not installed on the server.'}), 501
    secret = os.environ.get('STRIPE_SECRET_KEY', '').strip()
    if not secret:
        return jsonify({'error': 'Stripe is not configured (missing STRIPE_SECRET_KEY).'}), 501

    payload = request.get_json(silent=True) or {}
    try:
        amount_dollars = int(payload.get('amount') or 0)
    except Exception:
        amount_dollars = 0

    allowed_amounts = {3, 5, 10, 20, 50}
    if amount_dollars not in allowed_amounts:
        return jsonify({'error': 'Invalid amount. Choose 3, 5, 10, 20, or 50.'}), 400

    currency = (os.environ.get('STRIPE_CURRENCY') or 'usd').strip().lower()
    product_name = (os.environ.get('STRIPE_PRODUCT_NAME') or 'PWHL Analytics - Coffee').strip()

    stripe.api_key = secret
    try:
        session = stripe.checkout.Session.create(
            mode='payment',
            line_items=[
                {
                    'price_data': {
                        'currency': currency,
                        'unit_amount': amount_dollars * 100,
                        'product_data': {
                            'name': product_name,
                        },
                    },
                    'quantity': 1,
                }
            ],
            success_url=url_for('coffee_page', _external=True) + '?success=1',
            cancel_url=url_for('coffee_page', _external=True) + '?canceled=1',
            metadata={
                'kind': 'coffee',
                'amount_dollars': str(amount_dollars),
            },
        )
        return jsonify({'url': session.url})
    except Exception:
        return jsonify({'error': 'Unable to create Stripe checkout session.'}), 500


@app.route('/api/stripe/webhook', methods=['POST'])
def stripe_webhook():
    """Stripe webhook receiver.

    Configure this endpoint in Stripe Dashboard and set:
      - STRIPE_WEBHOOK_SECRET (required for signature verification)

    Optional:
      - STRIPE_NOTIFY_WEBHOOK_URL: post a short message to a Discord/Slack webhook.
    """
    if stripe is None:
        return ('stripe not installed', 501)
    secret = (os.environ.get('STRIPE_WEBHOOK_SECRET') or '').strip()
    if not secret:
        return ('missing STRIPE_WEBHOOK_SECRET', 501)

    sig_header = request.headers.get('Stripe-Signature', '')
    payload = request.get_data(cache=False, as_text=False)
    try:
        event = stripe.Webhook.construct_event(payload=payload, sig_header=sig_header, secret=secret)
    except Exception:
        return ('invalid signature', 400)

    try:
        et = str(event.get('type') or '')
        if et in ('checkout.session.completed', 'checkout.session.async_payment_succeeded'):
            obj = (event.get('data') or {}).get('object') or {}
            amount_total = obj.get('amount_total')
            currency = (obj.get('currency') or '').upper()
            session_id = obj.get('id')
            created = obj.get('created')
            kind = (obj.get('metadata') or {}).get('kind')

            msg = f"[Stripe] Payment succeeded: kind={kind or 'unknown'} amount={amount_total} {currency} session={session_id} created={created}"
            print(msg, flush=True)

            notify_url = (os.environ.get('STRIPE_NOTIFY_WEBHOOK_URL') or '').strip()
            if notify_url:
                try:
                    # Keep payload simple so it works with Discord/Slack-style webhooks.
                    requests.post(notify_url, json={'content': msg}, timeout=3)
                except Exception:
                    pass
        return ('ok', 200)
    except Exception:
        return ('error', 500)


@app.route('/api/teams/filters')
def teams_filters():
    """Return teams + common filters for Teams page."""
    report_store.load()
    rows = report_store.rows
    teams = sorted({str(r.get('team_for')) for r in rows if r.get('team_for')}, key=str)
    if not teams and hasattr(data_api, 'teams') and data_api.teams:
        teams = sorted(data_api.teams.keys())
    seasons = sorted({str(r.get('season')) for r in rows if r.get('season')}, key=str)
    season_states = sorted({str(r.get('state')) for r in rows if r.get('state')}, key=str)
    strengths = sorted({str(r.get('strength')) for r in rows if r.get('strength')}, key=str)

    team_logos = {t: _team_logo_url(t) for t in teams}

    return jsonify({
        'teams': teams,
        'team_logos': team_logos,
        'seasons': seasons,
        'season_states': season_states,
        'strengths': strengths,
    })


@app.route('/api/teams/kpis')
def teams_kpis():
    """Return team KPIs for a selected team and filters.

    KPIs: GP, SF%, GF%, xGF%, Sh%, Sv%, PDO
    """
    team = request.args.get('team', '').strip()
    if not team:
        return jsonify({'error': 'team required'}), 400

    def _get_multi(name: str) -> List[str]:
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in (vals[0] or ''):
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    seasons_multi = _get_multi('seasons')
    season_states_multi = _get_multi('season_states')
    strengths_multi = _get_multi('strengths')

    season = request.args.get('season', 'All')
    season_state = request.args.get('season_state', 'All')
    strength = request.args.get('strength', 'All')

    # Back-compat: if multi params are present, prefer them.
    if seasons_multi:
        season = 'All'
    if season_states_multi:
        season_state = 'All'
    if strengths_multi:
        strength = 'All'

    report_store.load()
    rows = report_store.tables_teams(
        season=season,
        season_state=season_state,
        strength=strength,
        seasons_multi=seasons_multi,
        season_states_multi=season_states_multi,
        strengths_multi=strengths_multi,
    )
    rec = next((r for r in rows if r.get('Team') == team or r.get('team') == team), None)
    if not rec:
        return jsonify({
            'team': team,
            'logo': _team_logo_url(team),
            'GP': 0,
            'SF%': None,
            'GF%': None,
            'xGF%': None,
            'Sh%': None,
            'Sv%': None,
            'PDO': None,
        })

    logo = _team_logo_url(team)
    return jsonify({
        'team': team,
        'logo': logo,
        'GP': rec.get('GP'),
        'SF%': rec.get('SF%'),
        'GF%': rec.get('GF%'),
        'xGF%': rec.get('xGF%'),
        'Sh%': rec.get('Sh%'),
        'Sv%': rec.get('Sv%'),
        'PDO': rec.get('PDO'),
    })


@app.route('/api/teams/standings')
def teams_standings():
    """Return standings table for Teams page.

    Columns: Team, GP, Points (3-2-1-0), W, OTW, OTL, L, Pct, GF%, xGF%, PDO
    """
    def _get_multi(name: str) -> List[str]:
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in (vals[0] or ''):
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    seasons_multi = _get_multi('seasons')
    season_states_multi = _get_multi('season_states')
    strengths_multi = _get_multi('strengths')

    season = request.args.get('season', 'All')
    season_state = request.args.get('season_state', 'All')
    strength = request.args.get('strength', 'All')

    if seasons_multi:
        season = 'All'
    if season_states_multi:
        season_state = 'All'
    if strengths_multi:
        strength = 'All'

    report_store.load()
    rows = report_store.tables_teams(
        season=season,
        season_state=season_state,
        strength=strength,
        seasons_multi=seasons_multi,
        season_states_multi=season_states_multi,
        strengths_multi=strengths_multi,
    )

    def _norm(vals: List[str], fallback: str) -> List[str]:
        if not vals:
            return [fallback]
        if 'All' in vals:
            return ['All']
        # preserve order while unique
        out: List[str] = []
        seen = set()
        for v in vals:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    yrs = _norm(seasons_multi, season)
    sts = _norm(season_states_multi, season_state)
    points_by_team: Dict[str, Dict[str, Any]] = {}
    for yr in yrs:
        for st in sts:
            part = _compute_team_points_table(yr, st)
            for team_name, rec in (part or {}).items():
                agg = points_by_team.setdefault(team_name, {'GP': 0, 'W': 0, 'OTW': 0, 'OTL': 0, 'L': 0, 'Points': 0})
                for k in ('GP', 'W', 'OTW', 'OTL', 'L', 'Points'):
                    try:
                        agg[k] += int(rec.get(k) or 0)
                    except Exception:
                        pass

    for t, rec in points_by_team.items():
        gp = int(rec.get('GP') or 0)
        pts = int(rec.get('Points') or 0)
        rec['Pct'] = (pts / (gp * 3)) if gp > 0 else None

    out = []
    for r in rows:
        t = r.get('Team')
        pts_rec = points_by_team.get(t, {})
        gp_points = pts_rec.get('GP', 0)
        points = pts_rec.get('Points', 0)
        pct = pts_rec.get('Pct')
        out.append({
            'Team': t,
            'Logo': _team_logo_url(t),
            'GP': gp_points,
            'Points': points,
            'W': pts_rec.get('W', 0),
            'OTW': pts_rec.get('OTW', 0),
            'OTL': pts_rec.get('OTL', 0),
            'L': pts_rec.get('L', 0),
            'Pct': pct,
            'GF%': r.get('GF%'),
            'xGF%': r.get('xGF%'),
            'PDO': r.get('PDO'),
            # Extra metrics for charts (not displayed in standings table)
            'GP_metrics': r.get('GP'),
            'SF': r.get('SF'),
            'SA': r.get('SA'),
            'GF': r.get('GF'),
            'GA': r.get('GA'),
            'xGF': r.get('xGF'),
            'xGA': r.get('xGA'),
            'SF%': r.get('SF%'),
            'Sh%': r.get('Sh%'),
            'Sv%': r.get('Sv%'),
        })

    out.sort(key=lambda rr: (-(rr.get('Points') or 0), -(rr.get('Pct') or 0), rr.get('Team') or ''))
    return jsonify({'rows': out})


@app.route('/api/teams/performance')
def teams_performance():
    """Return per-game performance series for a selected team.

    Metrics per game: SF%, GF%, xGF%, Sh%, Sv%, PDO, plus raw counts.
    """
    team = request.args.get('team', '').strip()
    if not team:
        return jsonify({'error': 'team required'}), 400

    def _get_multi(name: str) -> List[str]:
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in (vals[0] or ''):
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    seasons_multi = _get_multi('seasons')
    season_states_multi = _get_multi('season_states')
    strengths_multi = _get_multi('strengths')

    season = request.args.get('season', 'All')
    season_state = request.args.get('season_state', 'All')
    strength_filter = request.args.get('strength', 'All')

    if seasons_multi:
        season = 'All'
    if season_states_multi:
        season_state = 'All'
    if strengths_multi:
        strength_filter = 'All'

    report_store.load()
    rows = report_store.rows
    if seasons_multi and 'All' not in seasons_multi:
        rows = [r for r in rows if r.get('season') in seasons_multi]
    elif season != 'All':
        rows = [r for r in rows if r.get('season') == season]

    if season_states_multi and 'All' not in season_states_multi:
        rows = [r for r in rows if r.get('state') in season_states_multi]
    elif season_state != 'All':
        rows = [r for r in rows if r.get('state') == season_state]
    # Only games where team participates
    rows = [r for r in rows if r.get('team_for') == team or r.get('team_against') == team]

    def _row_ok_for_strength(r, v_team: str, is_for: bool, sf: str) -> bool:
        if sf == 'All':
            return True
        s_class = report_store._classify_strength(r.get('strength', ''), v_team, is_for)
        if sf == 'EV':
            return s_class in ('5v5', 'EV')
        if sf in ('PP', 'SH', '5v5'):
            return s_class == sf
        return r.get('strength') == sf

    def row_ok(r, v_team: str, is_for: bool) -> bool:
        if strengths_multi:
            if 'All' in strengths_multi:
                return True
            return any(_row_ok_for_strength(r, v_team, is_for, sf) for sf in strengths_multi)
        return _row_ok_for_strength(r, v_team, is_for, strength_filter)

    by_game = {}

    def _infer_season_label(date_str: str) -> str:
        """Infer season label (e.g. '2023/2024') from an ISO-like date string."""
        if not date_str:
            return ''
        try:
            # Accept 'YYYY-MM-DD' or full ISO timestamps
            dt = datetime.fromisoformat(str(date_str)[:19])
            yr = int(dt.year)
            mo = int(dt.month)
            start = yr if mo >= 7 else (yr - 1)
            return f"{start}/{start+1}"
        except Exception:
            return ''

    for r in rows:
        gid = str(r.get('game_id') or '')
        if not gid:
            continue
        meta = (report_store.game_meta.get(gid, {}) or {})
        date = meta.get('date', '') or r.get('date', '') or ''
        season_label = meta.get('season', '') or r.get('season', '') or _infer_season_label(date)
        state_label = meta.get('state', '') or r.get('state', '') or ''
        rec = by_game.setdefault(gid, {
            'game_id': gid,
            'date': date,
            'season': season_label,
            'state': state_label,
            'opponent': '',
            'SF': 0, 'SA': 0, 'GF': 0, 'GA': 0,
            'xGF': 0.0, 'xGA': 0.0,
        })

        team_for = r.get('team_for')
        team_against = r.get('team_against')

        if team_for == team and row_ok(r, team, True):
            if r.get('is_shot'):
                rec['SF'] += 1
            if r.get('is_goal'):
                rec['GF'] += 1
            xv = r.get('xG')
            if xv not in (None, ''):
                try:
                    rec['xGF'] += float(xv)
                except Exception:
                    pass
            if not rec['opponent']:
                rec['opponent'] = str(team_against or '')

        if team_against == team and row_ok(r, team, False):
            if r.get('is_shot'):
                rec['SA'] += 1
            if r.get('is_goal'):
                rec['GA'] += 1
            xv = r.get('xG')
            if xv not in (None, ''):
                try:
                    rec['xGA'] += float(xv)
                except Exception:
                    pass
            if not rec['opponent']:
                rec['opponent'] = str(team_for or '')

    def pct(n, d):
        try:
            if d and d != 0:
                return round((float(n) / float(d)) * 100.0, 1)
        except Exception:
            return None
        return None

    series = []
    for gid, rec in by_game.items():
        sf, sa, gf, ga = rec['SF'], rec['SA'], rec['GF'], rec['GA']
        xgf = float(rec.get('xGF') or 0.0)
        xga = float(rec.get('xGA') or 0.0)
        sf_pct = pct(sf, sf + sa)
        gf_pct = pct(gf, gf + ga)
        xgf_pct = pct(xgf, xgf + xga)
        sh_pct = pct(gf, sf)
        sv_pct = pct((sa - ga), sa)
        pdo = round((sh_pct or 0) + (sv_pct or 0), 1) if (sh_pct is not None and sv_pct is not None) else None
        series.append({
            'game_id': gid,
            'date': rec.get('date', ''),
            'season': rec.get('season', ''),
            'state': rec.get('state', ''),
            'opponent': rec.get('opponent', ''),
            'SF': sf,
            'SA': sa,
            'GF': gf,
            'GA': ga,
            'xGF': round(xgf, 2),
            'xGA': round(xga, 2),
            'SF%': sf_pct,
            'GF%': gf_pct,
            'xGF%': xgf_pct,
            'Sh%': sh_pct,
            'Sv%': sv_pct,
            'PDO': pdo,
        })

    # Sort by date asc, then game_id
    series.sort(key=lambda r: (r.get('date', '') or '', r.get('game_id', '')))
    return jsonify({'team': team, 'series': series})

@app.route('/api/report/kpis')
def report_kpis():
    # Base single-value params
    params = {
        'team': request.args.get('team','All'),
        'strength': request.args.get('strength','All'),  # legacy single strength
        'season': request.args.get('season','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'perspective': request.args.get('perspective','For'),
    }
    # Multi-select helpers (accept repeated params OR single comma-separated string)
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    games = _get_multi('games')
    if games: params['games'] = ','.join(games)
    players = _get_multi('players')
    if players: params['players'] = ','.join(players)
    opponents = _get_multi('opponents')
    if opponents: params['opponents'] = ','.join(opponents)
    periods = _get_multi('periods')
    if periods: params['periods'] = ','.join(periods)
    events = _get_multi('events')
    if events: params['events'] = ','.join(events)
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    goalies = _get_multi('goalies')
    if goalies: params['goalies'] = ','.join(goalies)
    onice_multi = _get_multi('onice')
    if onice_multi: params['onice'] = ','.join(onice_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)
    data = report_store.compute_kpis(**params)
    return jsonify(data)

@app.route('/api/report/shotmap')
def report_shotmap():
    params = {
        'team': request.args.get('team','All'),
        'strength': request.args.get('strength','All'),
        'season': request.args.get('season','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'perspective': request.args.get('perspective','For'),
    }
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    games = _get_multi('games')
    if games: params['games'] = ','.join(games)
    players = _get_multi('players')
    if players: params['players'] = ','.join(players)
    opponents = _get_multi('opponents')
    if opponents: params['opponents'] = ','.join(opponents)
    periods = _get_multi('periods')
    if periods: params['periods'] = ','.join(periods)
    events = _get_multi('events')
    if events: params['events'] = ','.join(events)
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    goalies = _get_multi('goalies')
    if goalies: params['goalies'] = ','.join(goalies)
    onice_multi = _get_multi('onice')
    if onice_multi: params['onice'] = ','.join(onice_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)
    data = report_store.shotmap(**params)
    return jsonify(data)

@app.route('/api/report/tables')
def report_tables():
    table_type = request.args.get('type','skaters')
    by_game = request.args.get('by_game','').lower() == 'true'
    params = {
        'team': request.args.get('team','All'),
        'strength': request.args.get('strength','All'),
        'season': request.args.get('season','All'),
        'season_state': request.args.get('season_state','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'by_game': by_game,
    }
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    games = _get_multi('games')
    if games: params['games'] = ','.join(games)
    players = _get_multi('players')
    if players: params['players'] = ','.join(players)
    opponents = _get_multi('opponents')
    if opponents: params['opponents'] = ','.join(opponents)
    periods = _get_multi('periods')
    if periods: params['periods'] = ','.join(periods)
    events = _get_multi('events')
    if events: params['events'] = ','.join(events)
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    goalies = _get_multi('goalies')
    if goalies: params['goalies'] = ','.join(goalies)
    onice_multi = _get_multi('onice')
    if onice_multi: params['onice'] = ','.join(onice_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)
    if table_type in ('skaters','skaters_individual'):
        data=report_store.tables_skaters_individual(**params)
    elif table_type=='goalies':
        data=report_store.tables_goalies(**params)
    elif table_type=='teams':
        data=report_store.tables_teams(**params)
    else:
        data=[]
    return jsonify({'type': table_type, 'rows': data})

@app.route('/api/report/filters')
def report_filters():
    """Return option sets for multi-select slicers."""
    report_store.load()
    rows = report_store.rows
    # Build game labels using stored meta if available for home/away (preferred: Date Away at Home)
    # Collect games with dates and sort newest first (descending by date); blanks last
    game_ids = {r['game_id'] for r in rows if r['game_id']}
    game_meta_list = []
    for gid in game_ids:
        meta = report_store.game_meta.get(gid, {})
        date_str = meta.get('date','') or ''
        # Expect ISO YYYY-MM-DD; fallback sorts after real dates
        sort_key = date_str if date_str else '0000-00-00'
        game_meta_list.append((sort_key, gid, meta))
    game_meta_list.sort(key=lambda t: t[0], reverse=True)
    game_labels = []
    for _, gid, meta in game_meta_list:
        date = meta.get('date','')
        away = meta.get('away_team','') or ''
        home = meta.get('home_team','') or ''
        if away and home:
            label = f"{date} {away} at {home}"
        else:
            label = f"{date} {gid}"
        game_labels.append({'value': gid, 'label': label})
    players = sorted({r['shooter'] for r in rows if r.get('shooter')})
    goalies = sorted({r['goalie'] for r in rows if r.get('goalie')})
    periods = sorted({r['period'] for r in rows if r.get('period')})
    events = sorted({r['event'] for r in rows if r.get('event')})
    strengths = sorted({r['strength'] for r in rows if r.get('strength')})
    opp_teams = sorted({r['team_against'] for r in rows if r.get('team_against')})
    seasons = sorted({r['season'] for r in rows if r.get('season')})
    season_states = sorted({r['state'] for r in rows if r.get('state')})
    # On-ice player names set (distinct from shooter list). We union all on_ice_all lists.
    onice_players = sorted({p for r in rows for p in (r.get('on_ice_all') or [])})
    return jsonify({'games': game_labels,'players': players,'goalies': goalies,'periods': periods,'events': events,'strengths': strengths,'opponents': opp_teams,'seasons': seasons,'season_states': season_states,'onice': onice_players})

@app.route('/api/report/games')
def report_games():
    """Return only games that have data given current (non-game) filters.
    Games filter itself is ignored when determining availability so user can re-select.
    Accepts same multi-select params as other endpoints.
    """
    report_store.load()
    rows = report_store.rows
    team_param = request.args.get('team','').strip()
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    # Gather filters (exclude games)
    players = _get_multi('players')
    opponents = _get_multi('opponents')
    periods = _get_multi('periods')
    events = _get_multi('events')
    strengths_multi = _get_multi('strengths')
    goalies = _get_multi('goalies')
    onice_multi = _get_multi('onice')
    seasons_multi = _get_multi('seasons')
    season_states_multi = _get_multi('season_states')
    date_from = request.args.get('date_from','')
    date_to = request.args.get('date_to','')
    # Apply filters analogous to shotmap
    if seasons_multi:
        rows = [r for r in rows if r['season'] in seasons_multi]
    if season_states_multi:
        rows = [r for r in rows if r.get('state') in season_states_multi]
    if players:
        rows = [r for r in rows if r.get('shooter') in players]
    if opponents:
        rows = [r for r in rows if r['team_against'] in opponents or r['team_for'] in opponents]
    if periods:
        rows = [r for r in rows if r['period'] in periods]
    if events:
        rows = [r for r in rows if r['event'] in events]
    if strengths_multi:
        rows = [r for r in rows if r['strength'] in strengths_multi]
    if goalies:
        rows = [r for r in rows if r.get('goalie') in goalies]
    if onice_multi:
        rows = [r for r in rows if all(p in (r.get('on_ice_all') or []) for p in onice_multi)]
    if date_from:
        rows = [r for r in rows if r['date'] >= date_from]
    if date_to:
        rows = [r for r in rows if r['date'] <= date_to]
    # If team perspective provided, restrict to games involving that team
    if team_param:
        rows = [r for r in rows if r['team_for']==team_param or r['team_against']==team_param]
    # Build and sort games newest first
    game_ids = {r['game_id'] for r in rows if r['game_id']}
    meta_list = []
    for gid in game_ids:
        meta = report_store.game_meta.get(gid, {})
        date_str = meta.get('date','') or ''
        sort_key = date_str if date_str else '0000-00-00'
        meta_list.append((sort_key, gid, meta))
    meta_list.sort(key=lambda t: t[0], reverse=True)
    out = []
    for _, gid, meta in meta_list:
        date = meta.get('date','')
        away = meta.get('away_team','') or ''
        home = meta.get('home_team','') or ''
        label = f"{date} {away} at {home}" if away and home else f"{date} {gid}"
        out.append({'value': gid, 'label': label})
    return jsonify({'games': out})

@app.route('/Teams.csv')
def teams_csv_raw():
    """Serve root Teams.csv so front-end color lookup succeeds (was 404)."""
    root_csv = os.path.join(app.root_path, 'Teams.csv')
    if os.path.exists(root_csv):
        return send_from_directory(app.root_path, 'Teams.csv', mimetype='text/csv')
    return jsonify({'error':'Teams.csv not found'}), 404

@app.route('/health')
def health():
    """Simple health check for deployment platforms (returns 200 JSON)."""
    return jsonify({'status':'ok'}), 200

@app.route('/hockey-rink.png')
def hockey_rink_image():
    """Serve the rink image from project root if present; otherwise fall back to logo.
    This avoids needing to relocate the binary into static/ while prototype evolves."""
    root_img = os.path.join(app.root_path, 'hockey-rink.png')
    if os.path.exists(root_img):
        return send_from_directory(app.root_path, 'hockey-rink.png', mimetype='image/png')
    return send_from_directory(os.path.join(app.root_path,'static'), 'PWHL_logo.png', mimetype='image/png')

@app.route('/api/report/reload', methods=['POST'])
def report_reload():
    report_store.load(force=True)
    return jsonify({'status':'reloaded','rows':len(report_store.rows)})

@app.route('/api/report/teams')
def report_teams():
    """Return the list of distinct teams present in the loaded report store."""
    report_store.load()
    teams = sorted({r['team_for'] for r in report_store.rows if r.get('team_for')})
    # Bootstrap: if no rows yet (e.g., Data not bundled on first deploy), fall back to Teams.csv list
    if not teams and hasattr(data_api, 'teams') and data_api.teams:
        teams = sorted(data_api.teams.keys())
    return jsonify({'teams': teams})

@app.route('/api/report/strengths')
def report_strengths():
    """Return unique strength strings present. Optional team parameter to scope to games involving that team."""
    team = request.args.get('team','').strip()
    report_store.load()
    rows = report_store.rows
    if team:
        rows = [r for r in rows if r['team_for']==team or r['team_against']==team]
    strengths = sorted({r['strength'] for r in rows if r['strength']})
    return jsonify({'strengths': strengths})

# -------- Skaters API --------
@app.route('/api/skaters/filters')
def skaters_filters():
    """Return players list and common filters for Skaters page."""
    import os
    import csv

    def _lineup_players_cached() -> list[str]:
        """Return sorted unique player names from Data/Lineups CSVs.

        Player slicer should be limited to players present in lineup exports.
        """
        # Cache lives on the function object to avoid module-level globals.
        cache = getattr(_lineup_players_cached, '_cache', None)
        sig = getattr(_lineup_players_cached, '_sig', None)

        lineups_dir = os.path.join(os.path.dirname(__file__), 'Data', 'Lineups')
        if not os.path.isdir(lineups_dir):
            return []

        # Build a cheap directory signature (count + max mtime) so we only re-scan
        # when files change.
        count = 0
        max_mtime = 0.0
        try:
            for ent in os.scandir(lineups_dir):
                if not ent.is_file() or not ent.name.endswith('.csv'):
                    continue
                count += 1
                try:
                    max_mtime = max(max_mtime, ent.stat().st_mtime)
                except Exception:
                    pass
        except Exception:
            return []

        cur_sig = (count, int(max_mtime))
        if cache is not None and sig == cur_sig:
            return cache

        players_set: set[str] = set()
        for ent in os.scandir(lineups_dir):
            if not ent.is_file() or not ent.name.endswith('.csv'):
                continue
            try:
                with open(ent.path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        line_pos = (row.get('Line') or row.get('Position') or row.get('Pos') or '').strip().upper()
                        # Goalies appear in lineup exports with Line=G.
                        if line_pos == 'G' or line_pos == 'GOALIE':
                            continue
                        name = (row.get('Name') or row.get('player') or '').strip()
                        if name:
                            players_set.add(name)
            except Exception:
                continue

        players = sorted(players_set, key=str)
        setattr(_lineup_players_cached, '_cache', players)
        setattr(_lineup_players_cached, '_sig', cur_sig)
        return players

    report_store.load()
    rows = report_store.rows
    players = _lineup_players_cached()
    seasons = sorted({str(r.get('season')) for r in rows if r.get('season')}, key=str)
    season_states = sorted({str(r.get('state')) for r in rows if r.get('state')}, key=str)
    strengths = sorted({str(r.get('strength')) for r in rows if r.get('strength')}, key=str)
    return jsonify({'players': players, 'seasons': seasons, 'season_states': season_states, 'strengths': strengths})


# -------- Goalies API --------
@app.route('/api/goalies/filters')
def goalies_filters():
    """Return goalies list and common filters for Goalies page."""
    import os
    import csv

    def _lineup_goalies_cached() -> list[str]:
        """Return sorted unique goalie names from Data/Lineups CSVs."""
        cache = getattr(_lineup_goalies_cached, '_cache', None)
        sig = getattr(_lineup_goalies_cached, '_sig', None)

        lineups_dir = os.path.join(os.path.dirname(__file__), 'Data', 'Lineups')
        if not os.path.isdir(lineups_dir):
            return []

        count = 0
        max_mtime = 0.0
        try:
            for ent in os.scandir(lineups_dir):
                if not ent.is_file() or not ent.name.endswith('.csv'):
                    continue
                count += 1
                try:
                    max_mtime = max(max_mtime, ent.stat().st_mtime)
                except Exception:
                    pass
        except Exception:
            return []

        cur_sig = (count, int(max_mtime))
        if cache is not None and sig == cur_sig:
            return cache

        goalies_set: set[str] = set()
        for ent in os.scandir(lineups_dir):
            if not ent.is_file() or not ent.name.endswith('.csv'):
                continue
            try:
                with open(ent.path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        line_pos = (row.get('Line') or row.get('Position') or row.get('Pos') or '').strip().upper()
                        if line_pos != 'G' and line_pos != 'GOALIE':
                            continue
                        name = (row.get('Name') or row.get('player') or '').strip()
                        if name:
                            goalies_set.add(name)
            except Exception:
                continue

        goalies = sorted(goalies_set, key=str)
        setattr(_lineup_goalies_cached, '_cache', goalies)
        setattr(_lineup_goalies_cached, '_sig', cur_sig)
        return goalies

    report_store.load()
    rows = report_store.rows
    goalies = _lineup_goalies_cached()
    seasons = sorted({str(r.get('season')) for r in rows if r.get('season')}, key=str)
    season_states = sorted({str(r.get('state')) for r in rows if r.get('state')}, key=str)
    strengths = sorted({str(r.get('strength')) for r in rows if r.get('strength')}, key=str)
    return jsonify({'players': goalies, 'seasons': seasons, 'season_states': season_states, 'strengths': strengths})


@app.route('/api/goalies/stats')
def goalies_stats():
    """Return basic goalie stats given filters.

    KPIs:
      GP, TOI, SA, GA, Sv%, xGA, xSv%, dSv%, GSAx
    Definitions:
      SA counts SOG only (Shot + Goal) where goalie matches.
      xGA is sum of xG for those SA events.
      GSAx = xGA - GA.
    """
    goalie = request.args.get('player', '').strip()
    if not goalie:
        return jsonify({'error': 'player required'}), 400

    params = {
        'team': 'All',
        'season': request.args.get('season', 'All'),
        'season_state': request.args.get('season_state', 'All'),
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'segment': request.args.get('segment', 'all'),
        'strength': request.args.get('strength', 'All'),
    }

    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    strengths_multi = _get_multi('strengths')
    if strengths_multi:
        params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi:
        params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi:
        params['season_states_multi'] = ','.join(season_states_multi)

    report_store.load()
    rows = report_store.pbp_rows(**params)

    # Load lineup TOI for relevant games
    game_ids = sorted({str(r.get('game_id')) for r in rows if r.get('game_id')}, key=str)
    for gid in game_ids:
        try:
            report_store._load_lineups_for_game(gid)
        except Exception:
            pass

    games_played = {gid for gid in game_ids if report_store.toi_lookup.get((gid, goalie), 0) > 0}
    gp = len(games_played)
    total_toi_secs = sum(
        secs for (gid, name), secs in report_store.toi_lookup.items()
        if name == goalie and gid in games_played
    )
    toi = round(total_toi_secs / 60, 1) if total_toi_secs else 0.0

    faced = [r for r in rows if r.get('goalie') == goalie and r.get('event') in ('Shot', 'Goal')]
    sa = len(faced)
    ga = sum(1 for r in faced if r.get('event') == 'Goal')
    xga = round(sum(float(r.get('xG') or 0.0) for r in faced), 2)

    sv_pct = round(((sa - ga) / sa * 100.0) if sa else 0.0, 1)
    xsv_pct = round(((1.0 - (xga / sa)) * 100.0) if sa else 0.0, 1)
    dsv_pct = round(sv_pct - xsv_pct, 1)
    gsax = round(xga - ga, 2)

    # Best-effort team: use the most recent game the goalie actually played.
    team = ''
    latest_gid = ''
    if games_played:
        latest_gid = max(
            games_played,
            key=lambda g: (report_store.game_meta.get(str(g), {}).get('date', '') or '')
        )
    if latest_gid:
        try:
            import os
            import csv
            lineups_dir = os.path.join(os.path.dirname(__file__), 'Data', 'Lineups')
            lineup_file = os.path.join(lineups_dir, f"{latest_gid}_teams.csv")
            if os.path.exists(lineup_file):
                with open(lineup_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if (row.get('Name') or '').strip() == goalie:
                            team = (row.get('Team') or '').strip()
                            break
        except Exception:
            pass

    def _calc_age(birth_date_str: str):
        if not birth_date_str:
            return None
        try:
            b = datetime.strptime(birth_date_str[:10], '%Y-%m-%d').date()
            today = datetime.utcnow().date()
            years = today.year - b.year - (1 if (today.month, today.day) < (b.month, b.day) else 0)
            return max(0, years)
        except Exception:
            return None

    profile = {'team': team, 'jersey': '', 'position': 'G', 'birthDate': '', 'age': None}
    if games_played:
        gids = sorted(games_played, key=lambda g: report_store.game_meta.get(str(g), {}).get('date', ''), reverse=True)
        for gid in gids[:8]:
            summary = data_api.fetch_game_summary(gid)
            if not summary:
                continue
            try:
                for side in ('homeTeam', 'visitingTeam'):
                    t = summary.get(side) or {}
                    tname = t.get('name') or ''
                    for group in ('goalies', 'skaters'):
                        for p in t.get(group, []):
                            if p.get('name') != goalie:
                                continue
                            profile['jersey'] = str(p.get('jersey') or '')
                            profile['position'] = str(p.get('position') or 'G')
                            profile['birthDate'] = str(p.get('birthDate') or '')
                            profile['age'] = _calc_age(profile['birthDate'])
                            if tname and tname not in ('Home Team', 'Visiting Team'):
                                profile['team'] = tname
                            raise StopIteration
            except StopIteration:
                break
            except Exception:
                continue

    return jsonify({
        'player': goalie,
        'profile': profile,
        'stats': {
            'GP': gp,
            'TOI': toi,
            'SA': sa,
            'GA': ga,
            'Sv%': sv_pct,
            'xGA': xga,
            'xSv%': xsv_pct,
            'dSv%': dsv_pct,
            'GSAx': gsax,
        }
    })


@app.route('/api/goalies/shotmap')
def goalies_shotmap():
    """Return goalie shot attempts against with coordinates for heat map."""
    goalie = request.args.get('player', '').strip()
    if not goalie:
        return jsonify({'attempts': []})

    params = {
        'team': 'All',
        'season': request.args.get('season', 'All'),
        'season_state': request.args.get('season_state', 'All'),
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'segment': request.args.get('segment', 'all'),
        'strength': request.args.get('strength', 'All'),
        'goalies': goalie,
    }

    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    strengths_multi = _get_multi('strengths')
    if strengths_multi:
        params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi:
        params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi:
        params['season_states_multi'] = ','.join(season_states_multi)

    rows = report_store.pbp_rows(**params)
    attempts = [
        {
            'adj_x': r.get('adj_x'),
            'adj_y': r.get('adj_y'),
            'event': r.get('event'),
            'forTeam': r.get('team_for'),
            'againstTeam': r.get('team_against'),
            'strength': r.get('strength'),
            'shooter': r.get('shooter'),
            'goalie': r.get('goalie'),
            'period': r.get('period'),
            'xG': r.get('xG'),
        }
        for r in rows if r.get('goalie') == goalie and r.get('event') in ('Shot', 'Goal', 'Miss', 'Block')
    ]
    return jsonify({'attempts': attempts})


@app.route('/api/goalies/performance')
def goalies_performance():
    """Return per-game running totals for a single goalie.

    Metrics returned:
      - per_game: GA, SA, xGA, GSAx, TOI (minutes)
      - running:  running totals for the same metrics
    """
    goalie = request.args.get('player', '').strip()
    if not goalie:
        return jsonify({'error': 'player required'}), 400

    params = {
        'team': 'All',
        'season': request.args.get('season', 'All'),
        'season_state': request.args.get('season_state', 'All'),
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'segment': request.args.get('segment', 'all'),
        'strength': request.args.get('strength', 'All'),
    }

    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    strengths_multi = _get_multi('strengths')
    if strengths_multi:
        params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi:
        params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi:
        params['season_states_multi'] = ','.join(season_states_multi)

    report_store.load()
    rows = report_store.pbp_rows(**params)

    game_ids = sorted({str(r.get('game_id')) for r in rows if r.get('game_id')}, key=str)
    for gid in game_ids:
        try:
            report_store._load_lineups_for_game(gid)
        except Exception:
            pass
    games_played = [gid for gid in game_ids if report_store.toi_lookup.get((gid, goalie), 0) > 0]

    def _date_for(gid: str) -> str:
        return (report_store.game_meta.get(str(gid), {}) or {}).get('date', '') or ''

    games_played.sort(key=lambda g: (_date_for(str(g)) or '9999-99-99', int(str(g)) if str(g).isdigit() else str(g)))

    by_game = {str(gid): {'GA': 0, 'SA': 0, 'xGA': 0.0, 'GSAx': 0.0, 'TOI': 0.0} for gid in games_played}

    for gid in games_played:
        try:
            secs = int(report_store.toi_lookup.get((str(gid), goalie), 0) or 0)
        except Exception:
            secs = 0
        by_game[str(gid)]['TOI'] = round(secs / 60.0, 1) if secs else 0.0

    for r in rows:
        gid = str(r.get('game_id') or '')
        if gid not in by_game:
            continue
        if r.get('goalie') != goalie:
            continue
        ev = r.get('event')
        if ev in ('Shot', 'Goal'):
            by_game[gid]['SA'] += 1
            try:
                by_game[gid]['xGA'] += float(r.get('xG') or 0.0)
            except Exception:
                pass
        if ev == 'Goal':
            by_game[gid]['GA'] += 1

    for gid in list(by_game.keys()):
        by_game[gid]['xGA'] = round(float(by_game[gid].get('xGA') or 0.0), 2)
        by_game[gid]['GSAx'] = round(float(by_game[gid].get('xGA') or 0.0) - float(by_game[gid].get('GA') or 0.0), 2)

    running = {'GA': 0, 'SA': 0, 'xGA': 0.0, 'GSAx': 0.0, 'TOI': 0.0}
    out_games = []
    for idx, gid in enumerate(games_played, start=1):
        g = by_game.get(str(gid), {})
        running['GA'] += int(g.get('GA') or 0)
        running['SA'] += int(g.get('SA') or 0)
        running['xGA'] = round(float(running['xGA']) + float(g.get('xGA') or 0.0), 2)
        running['GSAx'] = round(float(running['GSAx']) + float(g.get('GSAx') or 0.0), 2)
        running['TOI'] = round(float(running['TOI']) + float(g.get('TOI') or 0.0), 1)

        meta = report_store.game_meta.get(str(gid), {}) or {}
        season = str(meta.get('season') or '')

        out_games.append({
            'gameNumber': idx,
            'gameId': str(gid),
            'date': _date_for(str(gid)) or '',
            'season': season,
            'per_game': {
                'GA': int(g.get('GA') or 0),
                'SA': int(g.get('SA') or 0),
                'xGA': round(float(g.get('xGA') or 0.0), 2),
                'GSAx': round(float(g.get('GSAx') or 0.0), 2),
                'TOI': round(float(g.get('TOI') or 0.0), 1),
            },
            'running': {
                'GA': running['GA'],
                'SA': running['SA'],
                'xGA': running['xGA'],
                'GSAx': running['GSAx'],
                'TOI': running['TOI'],
            }
        })

    return jsonify({'player': goalie, 'games': out_games})

@app.route('/api/skaters/stats')
def skaters_stats():
    """Return basic stats for a single player given filters."""
    player = request.args.get('player','').strip()
    if not player:
        return jsonify({'error':'player required'}), 400
    # Note: we intentionally do NOT pass `players=player` into pbp filtering here because
    # we need to count A1/A2 from goal rows where the player may be an assister.
    params = {
        'team': 'All',
        'season': request.args.get('season','All'),
        'season_state': request.args.get('season_state','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'strength': request.args.get('strength','All'),
    }
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)

    report_store.load()
    rows = report_store.pbp_rows(**params)

    # Load lineup TOI for relevant games so GP/TOI are correct even if player has no shots.
    game_ids = sorted({str(r.get('game_id')) for r in rows if r.get('game_id')}, key=str)
    for gid in game_ids:
        try:
            report_store._load_lineups_for_game(gid)
        except Exception:
            pass

    games_played = {gid for gid in game_ids if report_store.toi_lookup.get((gid, player), 0) > 0}
    gp = len(games_played)
    total_toi_secs = sum(
        secs for (gid, name), secs in report_store.toi_lookup.items()
        if name == player and gid in games_played
    )
    toi = round(total_toi_secs / 60, 1) if total_toi_secs else 0.0

    # Shots + xG for player's own shot attempts
    attempts = [r for r in rows if r.get('shooter') == player and r.get('event') in ('Shot', 'Goal')]
    shots = len(attempts)
    xg = round(sum(float(r.get('xG') or 0.0) for r in attempts), 2)

    # Goals, primary assists, secondary assists (dedup goals)
    def goal_key(r):
        return (
            r.get('game_id'), r.get('period'), r.get('timestamp'),
            r.get('shooter'), r.get('assist1'), r.get('assist2'),
            r.get('strength'), r.get('x'), r.get('y')
        )

    seen_goals = set()
    goals = 0
    a1 = 0
    a2 = 0
    for r in rows:
        if r.get('event') != 'Goal':
            continue
        gk = goal_key(r)
        if gk in seen_goals:
            continue
        seen_goals.add(gk)
        if r.get('shooter') == player:
            goals += 1
        if r.get('assist1') == player:
            a1 += 1
        if r.get('assist2') == player:
            a2 += 1

    points = goals + a1 + a2
    sh_pct = round((goals / shots * 100.0) if shots > 0 else 0.0, 1)

    # PIM: minutes are not present in our exported PBP CSV right now.
    # We approximate using 2 minutes per penalty taken.
    penalties_taken = sum(1 for r in rows if r.get('event') == 'Penalty' and r.get('shooter') == player)
    pim = penalties_taken * 2

    # Best-effort team: use the most recent game the player actually played.
    team = ''
    latest_gid = ''
    if games_played:
        latest_gid = max(
            games_played,
            key=lambda g: (report_store.game_meta.get(str(g), {}).get('date', '') or '')
        )
    if latest_gid:
        # Prefer lineup-derived team for that game
        try:
            lineups_dir = os.path.join(os.path.dirname(__file__), 'Data', 'Lineups')
            lineup_file = os.path.join(lineups_dir, f"{latest_gid}_teams.csv")
            if os.path.exists(lineup_file):
                with open(lineup_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if (row.get('Name') or '').strip() == player:
                            team = (row.get('Team') or '').strip()
                            break
        except Exception:
            pass
        # Fallback: infer from shot rows in that specific game
        if not team:
            for r in rows:
                if r.get('game_id') == latest_gid and r.get('shooter') == player and r.get('team_for'):
                    team = r.get('team_for')
                    break
    # Last resort: any team_for we see for shooter
    if not team:
        for r in rows:
            if r.get('shooter') == player and r.get('team_for'):
                team = r.get('team_for')
                break

    # Best-effort profile fields from Game Summary (jersey/position/birthDate/team)
    def _calc_age(birth_date_str: str):
        if not birth_date_str:
            return None
        try:
            # Common format from API: YYYY-MM-DD
            b = datetime.strptime(birth_date_str[:10], '%Y-%m-%d').date()
            today = datetime.utcnow().date()
            years = today.year - b.year - (1 if (today.month, today.day) < (b.month, b.day) else 0)
            return max(0, years)
        except Exception:
            return None

    profile = {'team': team, 'jersey': '', 'position': '', 'birthDate': '', 'age': None}
    if games_played:
        gids = sorted(games_played, key=lambda g: report_store.game_meta.get(str(g), {}).get('date', ''), reverse=True)
        for gid in gids[:8]:
            summary = data_api.fetch_game_summary(gid)
            if not summary:
                continue
            try:
                for side in ('homeTeam', 'visitingTeam'):
                    t = summary.get(side) or {}
                    tname = t.get('name') or ''
                    for group in ('skaters', 'goalies'):
                        for p in t.get(group, []):
                            if p.get('name') != player:
                                continue
                            profile['jersey'] = str(p.get('jersey') or '')
                            profile['position'] = str(p.get('position') or '')
                            profile['birthDate'] = str(p.get('birthDate') or '')
                            profile['age'] = _calc_age(profile['birthDate'])
                            # Only override inferred team if API provides a real team name.
                            if tname and tname not in ('Home Team', 'Visiting Team'):
                                profile['team'] = tname
                            raise StopIteration
            except StopIteration:
                break
            except Exception:
                continue

    return jsonify({
        'player': player,
        'profile': profile,
        'stats': {
            'GP': gp,
            'TOI': toi,
            'Shots': shots,
            'Goals': goals,
            'A1': a1,
            'A2': a2,
            'Points': points,
            'PIM': pim,
            'xG': xg,
            'Sh%': sh_pct,
        }
    })

@app.route('/api/skaters/shotmap')
def skaters_shotmap():
    """Return player's shot attempts with coordinates for heat map."""
    player = request.args.get('player','').strip()
    if not player:
        return jsonify({'attempts': []})
    params = {
        'team': 'All',
        'season': request.args.get('season','All'),
        'season_state': request.args.get('season_state','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'strength': request.args.get('strength','All'),
        'players': player,
    }
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)
    rows = report_store.pbp_rows(**params)
    # Filter to player's events and only shot-related types
    attempts = [
        {
            'adj_x': r.get('adj_x'), 'adj_y': r.get('adj_y'), 'event': r.get('event'),
            'forTeam': r.get('team_for'), 'againstTeam': r.get('team_against'),
            'strength': r.get('strength'), 'shooter': r.get('shooter'), 'goalie': r.get('goalie'),
            'period': r.get('period'),
            'xG': r.get('xG'),
        }
        for r in rows if r.get('shooter') == player and r.get('event') in ('Shot','Goal','Miss','Block')
    ]
    return jsonify({'attempts': attempts})


@app.route('/api/skaters/performance')
def skaters_performance():
    """Return per-game running totals for a single player.

    X-axis concept: gameNumber (1..N), ordered by game date ascending.
        Metrics returned:
            - per_game: Goals, Assists, Points, Shots, xG, GAx, PIM, TOI (minutes)
            - running:  running totals for the same metrics
    Notes:
      - Shots are SOG (Shot + Goal) for the player's own attempts.
      - xG is summed across those attempts.
      - GAx is computed as Goals - xG (shooting goals above expected).
      - PIM approximated as 2 minutes per Penalty event where player is p1_name (stored as shooter field).
    """
    player = request.args.get('player', '').strip()
    if not player:
        return jsonify({'error': 'player required'}), 400

    params = {
        'team': 'All',
        'season': request.args.get('season', 'All'),
        'season_state': request.args.get('season_state', 'All'),
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'segment': request.args.get('segment', 'all'),
        'strength': request.args.get('strength', 'All'),
    }

    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    strengths_multi = _get_multi('strengths')
    if strengths_multi:
        params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi:
        params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi:
        params['season_states_multi'] = ','.join(season_states_multi)

    report_store.load()
    rows = report_store.pbp_rows(**params)

    # Load lineup TOI for relevant games so we can identify games played.
    game_ids = sorted({str(r.get('game_id')) for r in rows if r.get('game_id')}, key=str)
    for gid in game_ids:
        try:
            report_store._load_lineups_for_game(gid)
        except Exception:
            pass
    games_played = [gid for gid in game_ids if report_store.toi_lookup.get((gid, player), 0) > 0]

    # Order games by date ascending (fallback to game_id if missing date).
    def _date_for(gid: str) -> str:
        return (report_store.game_meta.get(str(gid), {}) or {}).get('date', '') or ''
    games_played.sort(key=lambda g: (_date_for(str(g)) or '9999-99-99', int(str(g)) if str(g).isdigit() else str(g)))

    # Build per-game stats.
    by_game = {str(gid): {'Goals': 0, 'Assists': 0, 'Points': 0, 'Shots': 0, 'xG': 0.0, 'PIM': 0, 'TOI': 0.0} for gid in games_played}

    # TOI per game from lineup exports (seconds -> minutes)
    for gid in games_played:
        try:
            secs = int(report_store.toi_lookup.get((str(gid), player), 0) or 0)
        except Exception:
            secs = 0
        by_game[str(gid)]['TOI'] = round(secs / 60.0, 1) if secs else 0.0

    # Shots + xG per game (player's own attempts)
    for r in rows:
        gid = str(r.get('game_id') or '')
        if gid not in by_game:
            continue
        if r.get('shooter') != player:
            continue
        ev = r.get('event')
        if ev in ('Shot', 'Goal'):
            by_game[gid]['Shots'] += 1
            try:
                by_game[gid]['xG'] += float(r.get('xG') or 0.0)
            except Exception:
                pass

    # Goals + assists per game (dedup goal rows)
    def _goal_key(r):
        return (
            r.get('game_id'), r.get('period'), r.get('timestamp'),
            r.get('shooter'), r.get('assist1'), r.get('assist2'),
            r.get('strength'), r.get('x'), r.get('y')
        )

    seen_goals = set()
    for r in rows:
        if r.get('event') != 'Goal':
            continue
        gid = str(r.get('game_id') or '')
        if gid not in by_game:
            continue
        gk = _goal_key(r)
        if gk in seen_goals:
            continue
        seen_goals.add(gk)
        if r.get('shooter') == player:
            by_game[gid]['Goals'] += 1
        if r.get('assist1') == player:
            by_game[gid]['Assists'] += 1
        if r.get('assist2') == player:
            by_game[gid]['Assists'] += 1

    # PIM approximation: 2 minutes per Penalty event where player is p1_name (stored as shooter field)
    for r in rows:
        gid = str(r.get('game_id') or '')
        if gid not in by_game:
            continue
        if r.get('event') == 'Penalty' and r.get('shooter') == player:
            by_game[gid]['PIM'] += 2

    # Finalize per-game rounding + derived fields
    for gid in list(by_game.keys()):
        by_game[gid]['xG'] = round(float(by_game[gid].get('xG') or 0.0), 2)
        by_game[gid]['Points'] = int(by_game[gid].get('Goals') or 0) + int(by_game[gid].get('Assists') or 0)
        by_game[gid]['GAx'] = round(float(by_game[gid].get('Goals') or 0) - float(by_game[gid].get('xG') or 0.0), 2)

    # Build running totals series
    running = {'Goals': 0, 'Assists': 0, 'Points': 0, 'Shots': 0, 'xG': 0.0, 'GAx': 0.0, 'PIM': 0, 'TOI': 0.0}
    out_games = []
    for idx, gid in enumerate(games_played, start=1):
        g = by_game.get(str(gid), {})
        running['Goals'] += int(g.get('Goals') or 0)
        running['Assists'] += int(g.get('Assists') or 0)
        running['Points'] += int(g.get('Points') or 0)
        running['Shots'] += int(g.get('Shots') or 0)
        running['xG'] = round(float(running['xG']) + float(g.get('xG') or 0.0), 2)
        running['GAx'] = round(float(running['GAx']) + float(g.get('GAx') or 0.0), 2)
        running['PIM'] += int(g.get('PIM') or 0)
        running['TOI'] = round(float(running['TOI']) + float(g.get('TOI') or 0.0), 1)

        meta = report_store.game_meta.get(str(gid), {}) or {}
        season = str(meta.get('season') or '')

        out_games.append({
            'gameNumber': idx,
            'gameId': str(gid),
            'date': _date_for(str(gid)) or '',
            'season': season,
            'per_game': {
                'Goals': int(g.get('Goals') or 0),
                'Assists': int(g.get('Assists') or 0),
                'Points': int(g.get('Points') or 0),
                'Shots': int(g.get('Shots') or 0),
                'xG': round(float(g.get('xG') or 0.0), 2),
                'GAx': round(float(g.get('GAx') or 0.0), 2),
                'PIM': int(g.get('PIM') or 0),
                'TOI': round(float(g.get('TOI') or 0.0), 1),
            },
            'running': {
                'Goals': running['Goals'],
                'Assists': running['Assists'],
                'Points': running['Points'],
                'Shots': running['Shots'],
                'xG': running['xG'],
                'GAx': running['GAx'],
                'PIM': running['PIM'],
                'TOI': running['TOI'],
            }
        })

    return jsonify({'player': player, 'games': out_games})


@app.route('/api/skaters/goalies')
def skaters_goalies():
    """Return a goalie table for shots taken by a selected player.

    Columns: Goaltender, Goals, Shots, xG, Sh%, GAx (Goals - xG)
    Shots are SOG (Shot + Goal) taken by the player that have a goalie name.
    """
    player = request.args.get('player', '').strip()
    if not player:
        return jsonify({'goalies': []})

    params = {
        'team': 'All',
        'season': request.args.get('season', 'All'),
        'season_state': request.args.get('season_state', 'All'),
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'segment': request.args.get('segment', 'all'),
        'strength': request.args.get('strength', 'All'),
    }

    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals) == 1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]

    strengths_multi = _get_multi('strengths')
    if strengths_multi:
        params['strengths_multi'] = ','.join(strengths_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi:
        params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi:
        params['season_states_multi'] = ','.join(season_states_multi)

    rows = report_store.pbp_rows(**params)
    # Only shot attempts by this player, where a goalie is identified
    rows = [r for r in rows if r.get('shooter') == player and r.get('goalie')]

    by_goalie = {}
    for r in rows:
        ev = r.get('event')
        if ev not in ('Shot', 'Goal'):
            continue
        gname = (r.get('goalie') or '').strip()
        if not gname:
            continue
        rec = by_goalie.setdefault(gname, {'Goaltender': gname, 'Goals': 0, 'Shots': 0, 'xG': 0.0})
        rec['Shots'] += 1
        if ev == 'Goal':
            rec['Goals'] += 1
        try:
            rec['xG'] += float(r.get('xG') or 0.0)
        except Exception:
            pass

    out = []
    for rec in by_goalie.values():
        shots = rec['Shots']
        goals = rec['Goals']
        xg = round(rec.get('xG', 0.0), 2)
        sh_pct = round((goals / shots * 100.0) if shots else 0.0, 1)
        gax = round(goals - xg, 2)
        out.append({
            'Goaltender': rec['Goaltender'],
            'Goals': goals,
            'Shots': shots,
            'xG': xg,
            'Sh%': sh_pct,
            'GAx': gax,
        })

    # Default sort: GAx (desc), then xG (desc), then Shots (desc)
    out.sort(key=lambda r: (-r['GAx'], -r['xG'], -r['Shots'], r['Goaltender']))
    return jsonify({'goalies': out})

@app.route('/api/skaters/player_image')
def skaters_player_image():
    """Best-effort lookup of a player's image URL using recent game summaries.
    Scans games where the player appears, fetches summary, and returns the first image URL found.
    """
    player = request.args.get('player','').strip()
    if not player:
        return jsonify({'url': ''})
    report_store.load()
    # Search for recent games involving the player
    candidate_gids = [
        str(r.get('game_id'))
        for r in report_store.rows
        if r.get('game_id')
        and (
            r.get('shooter') == player
            or r.get('goalie') == player
            or player in (r.get('on_ice_all') or [])
        )
    ]
    # Sort by date descending using meta
    unique_gids = sorted(set(candidate_gids), key=lambda g: report_store.game_meta.get(str(g), {}).get('date', ''), reverse=True)
    for gid in unique_gids[:8]:  # check a few recent games
        summary = data_api.fetch_game_summary(gid)
        if not summary:
            continue
        try:
            for side in ('homeTeam','visitingTeam'):
                team = summary.get(side) or {}
                for group in ('skaters','goalies'):
                    for p in team.get(group, []):
                        if p.get('name') == player:
                            url = p.get('playerImageURL') or ''
                            if url:
                                # Prefer 240x240 assets if available
                                if 'assets.leaguestat.com' in url and '/120x160/' in url:
                                    url = url.replace('/120x160/', '/240x240/')
                                return jsonify({'url': url})
                            # Try constructing from player ID if available
                            pid = p.get('playerId') or p.get('id') or ''
                            if isinstance(pid, (int,str)) and str(pid).isdigit():
                                return jsonify({'url': f"https://assets.leaguestat.com/pwhl/240x240/{pid}.jpg"})
        except Exception:
            continue
    return jsonify({'url': ''})


@app.route('/api/goalies/player_image')
def goalies_player_image():
    """Alias for player image lookup used by Goalies page."""
    return skaters_player_image()

# -------- Data API (reuses report_store) --------
@app.route('/api/data/pbp')
def data_pbp():
    """Return filtered Play-by-Play rows for Data page.

    Accepts same filters as report endpoints, except 'team' means participation (either side).
    """
    params = {
        'team': request.args.get('team','All'),
        'season': request.args.get('season','All'),
        'season_state': request.args.get('season_state','All'),
        'date_from': request.args.get('date_from',''),
        'date_to': request.args.get('date_to',''),
        'segment': request.args.get('segment','all'),
        'strength': request.args.get('strength','All'),
    }
    # Multi-select helpers
    def _get_multi(name):
        vals = request.args.getlist(name)
        if len(vals)==1 and ',' in vals[0]:
            vals = [v for v in vals[0].split(',') if v]
        return [v for v in vals if v]
    games = _get_multi('games')
    if games: params['games'] = ','.join(games)
    players = _get_multi('players')
    if players: params['players'] = ','.join(players)
    opponents = _get_multi('opponents')
    if opponents: params['opponents'] = ','.join(opponents)
    periods = _get_multi('periods')
    if periods: params['periods'] = ','.join(periods)
    events = _get_multi('events')
    if events: params['events'] = ','.join(events)
    strengths_multi = _get_multi('strengths')
    if strengths_multi: params['strengths_multi'] = ','.join(strengths_multi)
    goalies = _get_multi('goalies')
    if goalies: params['goalies'] = ','.join(goalies)
    onice_multi = _get_multi('onice')
    if onice_multi: params['onice'] = ','.join(onice_multi)
    seasons_multi = _get_multi('seasons')
    if seasons_multi: params['seasons_multi'] = ','.join(seasons_multi)
    season_states_multi = _get_multi('season_states')
    if season_states_multi: params['season_states_multi'] = ','.join(season_states_multi)
    # Execute
    rows = report_store.pbp_rows(**params)
    return jsonify({'rows': rows, 'count': len(rows)})

@app.route('/favicon.ico')
def favicon():
    # Prefer a real .ico if present; else try favicon.png; else fall back to PWHL_logo.png
    root = app.root_path
    static_dir = os.path.join(app.root_path, 'static')
    ico_root = os.path.join(root, 'favicon.ico')
    ico_static = os.path.join(static_dir, 'favicon.ico')
    png_root = os.path.join(root, 'favicon.png')
    png_static = os.path.join(static_dir, 'favicon.png')

    try:
        if os.path.exists(ico_root):
            return send_from_directory(root, 'favicon.ico', mimetype='image/x-icon')
        if os.path.exists(ico_static):
            return send_from_directory(static_dir, 'favicon.ico', mimetype='image/x-icon')
        if os.path.exists(png_root):
            return send_from_directory(root, 'favicon.png', mimetype='image/png')
        if os.path.exists(png_static):
            return send_from_directory(static_dir, 'favicon.png', mimetype='image/png')
    except Exception:
        pass

    return send_from_directory(static_dir, 'PWHL_logo.png', mimetype='image/png')

@app.route('/favicon.png')
def favicon_png():
    # Serve root-level favicon.png if present (user-provided smaller icon)
    return send_from_directory(app.root_path, 'favicon.png', mimetype='image/png')

@app.route('/game/<int:game_id>')
def game_page(game_id):
    """Game details page"""
    return render_template('game.html')

@app.route('/api/game/info/<int:game_id>')
def get_game_info(game_id):
    """Get basic game information for title and display"""
    try:
        # Search through all seasons to find the game
        game_info = None
        
        for season in data_api.all_seasons:
            games_data = data_api.fetch_schedule_data(season)
            parsed_games = data_api.parse_games_data(games_data, season)
            
            # Look for the specific game
            for game in parsed_games:
                if str(game['game_id']) == str(game_id):
                    game_info = game
                    break
            
            if game_info:
                break
        
        if not game_info:
            return jsonify({'error': 'Game not found'}), 404
        
        return jsonify({
            'game_id': game_info['game_id'],
            'date': game_info['date'],
            'home_team': game_info['home_team'],  # Already full team name
            'away_team': game_info['away_team'],  # Already full team name
            'home_score': game_info.get('home_score', ''),
            'away_score': game_info.get('away_score', ''),
            'status': game_info['status'],
            'season_year': game_info['season_year'],
            'season_state': game_info['season_state'],
            'home_team_logo': game_info.get('home_team_logo', ''),  # Already included
            'away_team_logo': game_info.get('away_team_logo', ''),   # Already included
            'home_team_id': str(game_info.get('home_team_id', '')),
            'away_team_id': str(game_info.get('away_team_id', ''))
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/seasons')
def get_seasons():
    return jsonify({
        'season_years': data_api.season_years,
        'season_states': data_api.season_states,
        'season_mapping': data_api.season_mapping
    })

@app.route('/api/schedule')
def get_schedule():
    season_year = request.args.get('season_year', 'All')
    season_state = request.args.get('season_state', 'All')
    team = request.args.get('team', 'All')
    status = request.args.get('status', 'All')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    # Determine which seasons to fetch
    seasons_to_fetch = []
    
    if season_year == 'All':
        # Fetch all available seasons
        seasons_to_fetch = data_api.all_seasons
    else:
        # Fetch specific season(s)
        if season_state == 'All':
            # Get both regular season and playoffs for the year
            if season_year in data_api.season_mapping:
                for state, season_id in data_api.season_mapping[season_year].items():
                    if season_id in data_api.all_seasons:  # Only fetch if season exists
                        seasons_to_fetch.append(season_id)
        else:
            # Get specific season and state combination
            if season_year in data_api.season_mapping and season_state in data_api.season_mapping[season_year]:
                season_id = data_api.season_mapping[season_year][season_state]
                if season_id in data_api.all_seasons:  # Only fetch if season exists
                    seasons_to_fetch.append(season_id)
    
    # Fetch and combine data from all relevant seasons
    all_games = []
    for season_id in seasons_to_fetch:
        games_data = data_api.fetch_schedule_data(season_id)
        parsed_games = data_api.parse_games_data(games_data, season_id)
        all_games.extend(parsed_games)
    
    # Apply filters
    filtered_games = []
    for game in all_games:
        # Season year filter (already handled in fetch logic, but double-check)
        if season_year != 'All' and game['season_year'] != season_year:
            continue
            
        # Season state filter
        if season_state != 'All' and game['season_state'] != season_state:
            continue
        
        # Team filter
        if team != 'All' and game['home_team'] != team and game['away_team'] != team:
            continue
        
        # Status filter
        if status != 'All' and game['status'] != status:
            continue
        if date_from and game['full_date']:
            try:
                if game['full_date'] < date_from:
                    continue
            except:
                pass
                
        if date_to and game['full_date']:
            try:
                if game['full_date'] > date_to:
                    continue
            except:
                pass
        
        filtered_games.append(game)
    
    # Sort games by date
    filtered_games.sort(key=lambda x: x['date_obj'] if pd.notna(x['date_obj']) else pd.Timestamp.min)
    
    # Get unique values for filters
    all_teams = set()
    all_statuses = set()
    
    for game in all_games:
        if game['home_team']:
            all_teams.add(game['home_team'])
        if game['away_team']:
            all_teams.add(game['away_team'])
        if game['status']:
            all_statuses.add(game['status'])
    
    return jsonify({
        'games': filtered_games,
        'filters': {
            'teams': sorted(list(all_teams)),
            'statuses': sorted(list(all_statuses))
        },
        'stats': {
            'total_games': len(filtered_games),
            'completed_games': len([g for g in filtered_games if 'Final' in g['status']]),
            'pending_games': len([g for g in filtered_games if 'Final' not in g['status']])
        }
    })

@app.route('/api/game/summary/<int:game_id>')
def get_game_summary(game_id):
    """Get game summary/lineup data for a specific game"""
    summary_data = data_api.fetch_game_summary(game_id)
    
    if summary_data is None:
        return jsonify({'error': 'Game summary not found'}), 404
    
    return jsonify(summary_data)

@app.route('/api/export/lineups/<int:game_id>.csv')
def export_lineups_csv(game_id: int):
    # Find game info
    try:
        games = []
        for season_id in data_api.all_seasons:
            parsed = data_api.parse_games_data(data_api.fetch_schedule_data(season_id), season_id)
            games.extend(parsed)
        game_info = next((g for g in games if str(g.get('game_id')) == str(game_id)), None)
        if not game_info:
            return jsonify({'error':'Game not found'}), 404
        summary_data = data_api.fetch_game_summary(game_id)
        if not isinstance(summary_data, dict):
            return jsonify({'error':'Summary unavailable'}), 404
        # Build team color mapping from Teams.csv already loaded in data_api
        team_color_by_name = { name: (t.get('color') or '') for name, t in data_api.teams.items() }
        team_color_by_id = { str(t.get('id') or ''): (t.get('color') or '') for t in data_api.teams.values() }
        csv_text = generate_lineups_csv(game_info, summary_data, team_color_by_name, team_color_by_id)
        return Response(csv_text, mimetype='text/csv; charset=utf-8', headers={'Content-Disposition': f'attachment; filename="{game_id}_teams.csv"'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/pbp/<int:game_id>.csv')
def export_pbp_csv(game_id: int):
    try:
        games = []
        for season_id in data_api.all_seasons:
            parsed = data_api.parse_games_data(data_api.fetch_schedule_data(season_id), season_id)
            games.extend(parsed)
        game_info = next((g for g in games if str(g.get('game_id')) == str(game_id)), None)
        if not game_info:
            return jsonify({'error':'Game not found'}), 404
        pbp_data = data_api.fetch_play_by_play(game_id)
        if not isinstance(pbp_data, list):
            return jsonify({'error':'Play-by-play unavailable'}), 404
        # Also fetch summary for lineup-based shootout inference
        summary_data = data_api.fetch_game_summary(game_id)
        # Build team code/name maps from Teams.csv to assist mapping when numeric ids are missing
        code_to_name = { (t.get('team_code') or ''): name for name, t in data_api.teams.items() if t.get('team_code') }
        name_to_code = { name: (t.get('team_code') or '') for name, t in data_api.teams.items() if t.get('team_code') }
        teams_meta = { 'code_to_name': code_to_name, 'name_to_code': name_to_code }
        csv_text = generate_pbp_csv(game_info, pbp_data, summary_data if isinstance(summary_data, dict) else None, teams_meta)
        return Response(csv_text, mimetype='text/csv; charset=utf-8', headers={'Content-Disposition': f'attachment; filename="{game_id}_shots.csv"'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/game/summary/test/<int:game_id>')
def get_game_summary_test(game_id):
    """Test endpoint with sample data to demonstrate functionality"""
    # Sample data structure that matches what we expect from the real API
    sample_data = {
        "homeTeam": {
            "name": "Toronto Sceptres",
            "goalies": [
                {
                    "info": {
                        "firstName": "Kristen",
                        "lastName": "Campbell", 
                        "jerseyNumber": "33",
                        "position": "G",
                        "birthDate": "1992-01-15",
                        "playerImageURL": "https://assets.leaguestat.com/pwhl/120x160/33.jpg"
                    },
                    "stats": {
                        "gamesPlayed": 15,
                        "wins": 8,
                        "losses": 5,
                        "saves": 387,
                        "savePct": 0.915
                    }
                }
            ],
            "skaters": [
                {
                    "info": {
                        "firstName": "Sarah",
                        "lastName": "Nurse",
                        "jerseyNumber": "20", 
                        "position": "LW",
                        "birthDate": "1995-01-04",
                        "playerImageURL": "https://assets.leaguestat.com/pwhl/120x160/75.jpg"
                    },
                    "stats": {
                        "goals": 12,
                        "assists": 18,
                        "points": 30,
                        "penaltyMinutes": 8,
                        "plusMinus": 5
                    }
                },
                {
                    "info": {
                        "firstName": "Emma",
                        "lastName": "Maltais",
                        "jerseyNumber": "27",
                        "position": "C", 
                        "birthDate": "1999-11-04",
                        "playerImageURL": "https://assets.leaguestat.com/pwhl/120x160/73.jpg"
                    },
                    "stats": {
                        "goals": 8,
                        "assists": 15,
                        "points": 23,
                        "penaltyMinutes": 12,
                        "plusMinus": 3
                    }
                }
            ]
        },
        "visitingTeam": {
            "name": "Montreal Victoire",
            "goalies": [
                {
                    "info": {
                        "firstName": "Ann-Renée",
                        "lastName": "Desbiens",
                        "jerseyNumber": "30",
                        "position": "G",
                        "birthDate": "1994-08-29", 
                        "playerImageURL": "https://assets.leaguestat.com/pwhl/120x160/30.jpg"
                    },
                    "stats": {
                        "gamesPlayed": 18,
                        "wins": 10,
                        "losses": 6,
                        "saves": 423,
                        "savePct": 0.908
                    }
                }
            ],
            "skaters": [
                {
                    "info": {
                        "firstName": "Marie-Philip",
                        "lastName": "Poulin",
                        "jerseyNumber": "29",
                        "position": "C",
                        "birthDate": "1991-03-28",
                        "playerImageURL": "https://assets.leaguestat.com/pwhl/120x160/29.jpg"
                    },
                    "stats": {
                        "goals": 15,
                        "assists": 22,
                        "points": 37,
                        "penaltyMinutes": 6,
                        "plusMinus": 8
                    }
                }
            ]
        }
    }
    
    # Process the sample data using our expansion logic
    processed_data = data_api.process_game_summary_data(sample_data)
    
    return jsonify(processed_data)

@app.route('/api/game/playbyplay/<int:game_id>')
def get_play_by_play(game_id):
    """Get play-by-play data for a specific game"""
    pbp_data = data_api.fetch_play_by_play(game_id)
    
    if pbp_data is None:
        return jsonify({'error': 'Play-by-play data not found'}), 404
    
    return jsonify(pbp_data)

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=8501)