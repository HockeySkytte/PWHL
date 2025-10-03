from flask import Flask, render_template, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import requests
import json
import pandas as pd
from datetime import datetime
import csv
import os
import time

# Simple in-memory cache for aggregated report events
report_cache = {
    'events': [],  # list of parsed pbp events from Data/Play-by-Play CSVs
    'mtimes': {},  # filepath -> last modified time when cached
    'last_load': 0.0,
}

app = Flask(__name__)
CORS(app)

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

def _scan_pbp_data_folder() -> list[dict]:
    """Scan Data/Play-by-Play folder and parse *_shots.csv into event dicts.
    Cached aggressively; re-reads only files whose mtime changed since last load.
    """
    base = os.path.join(app.root_path, 'Data', 'Play-by-Play')
    if not os.path.isdir(base):
        return []
    updated_events: list[dict] = []
    # Track whether we need full rebuild (if any disappeared) – simplest approach: rebuild always if counts differ
    try:
        file_paths = [os.path.join(base, f) for f in os.listdir(base) if f.endswith('_shots.csv')]
    except Exception:
        return []
    # Check mtimes
    global report_cache
    cache_mtimes = report_cache.get('mtimes', {})
    need_reload = False
    if len(cache_mtimes) != len(file_paths):
        need_reload = True
    else:
        for fp in file_paths:
            try:
                mt = os.path.getmtime(fp)
            except Exception:
                need_reload = True
                break
            if cache_mtimes.get(fp) != mt:
                need_reload = True
                break
    if not need_reload and report_cache.get('events'):
        return report_cache['events']
    # Rebuild
    new_mtimes: dict[str, float] = {}
    for fp in sorted(file_paths):
        try:
            mt = os.path.getmtime(fp)
            new_mtimes[fp] = mt
            with open(fp, 'r', encoding='utf-8') as f:
                import csv
                r = csv.DictReader(f)
                for row in r:
                    # Normalize & filter minimal fields we need client side
                    # Expect columns: id,timestamp,event,team,venue,team_home,team_away,period,perspective,strength,p1_no,p1_name,...,x,y,game_id,game_date
                    ev_type = (row.get('event') or '').strip()
                    if not ev_type:
                        continue
                    # Convert coordinates to float if possible
                    def to_float(v):
                        try:
                            return float(v)
                        except Exception:
                            return None
                    x = to_float(row.get('x'))
                    y = to_float(row.get('y'))
                    period = row.get('period') or ''
                    # Shooting / goal flags
                    is_goal = ev_type.lower() == 'goal'
                    is_shot_family = ev_type.lower() in ('shot','goal')
                    strength = row.get('strength') or ''
                    updated_events.append({
                        'game_id': row.get('game_id') or '',
                        'timestamp': row.get('timestamp') or row.get('time') or '',
                        'event': ev_type,
                        'team': row.get('team') or '',
                        'venue': row.get('venue') or '',
                        'period': period,
                        'strength': strength,
                        'player': row.get('p1_name') or '',
                        'player_no': row.get('p1_no') or '',
                        'goalie': row.get('goalie_name') or '',
                        'x': x,
                        'y': y,
                        'is_goal': is_goal,
                        'is_shot': is_shot_family,
                        'game_date': row.get('game_date') or '',
                    })
        except Exception as e:
            print(f"Error parsing {fp}: {e}")
    report_cache['events'] = updated_events
    report_cache['mtimes'] = new_mtimes
    report_cache['last_load'] = time.time()
    return updated_events

def _aggregate_report(events: list[dict]) -> dict:
    """Compute lightweight aggregate metrics for report tab."""
    shots = sum(1 for e in events if e.get('is_shot'))
    goals = sum(1 for e in events if e.get('is_goal'))
    blocks = sum(1 for e in events if str(e.get('event','')).lower() == 'block')
    corsi = shots + blocks  # (Shots incl goals) + blocks
    fenwick = shots  # Without blocks (already just shots/goals set)
    sh_pct = round((goals / shots * 100.0), 1) if shots else 0.0
    teams = sorted({e.get('team') for e in events if e.get('team')})
    periods = sorted({e.get('period') for e in events if e.get('period')})
    strengths = sorted({e.get('strength') for e in events if e.get('strength')})
    players = sorted({e.get('player') for e in events if e.get('player')})
    return {
        'totals': {
            'shots': shots,
            'goals': goals,
            'blocks': blocks,
            'corsi': corsi,
            'fenwick': fenwick,
            'sh_pct': sh_pct,
        },
        'filters': {
            'teams': teams,
            'periods': periods,
            'strengths': strengths,
            'players': players,
            'events': sorted({e.get('event') for e in events if e.get('event')})
        }
    }

@app.route('/api/report/events')
def api_report_events():
    try:
        events = _scan_pbp_data_folder()
        agg = _aggregate_report(events)
        # Optional filtering via query params
        q_team = (request.args.get('team') or '').strip().lower()
        q_player = (request.args.get('player') or '').strip().lower()
        q_event = (request.args.get('event') or '').strip().lower()
        q_period = (request.args.get('period') or '').strip().lower()
        q_strength = (request.args.get('strength') or '').strip().lower()
        filtered = []
        for e in events:
            if q_team and e.get('team','').lower() != q_team: continue
            if q_player and e.get('player','').lower() != q_player: continue
            if q_event and e.get('event','').lower() != q_event: continue
            if q_period and str(e.get('period','')).lower() != q_period: continue
            if q_strength and e.get('strength','').lower() != q_strength: continue
            filtered.append(e)
        return jsonify({'events': filtered, 'aggregate': agg, 'count': len(filtered)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

class PWHLDataAPI:
    def __init__(self):
        pass  # placeholder (existing definition is earlier; we won't duplicate logic here)

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/report')
def report_page():
    # Serve the static report HTML (no API dependencies for data)
    return render_template('report/report.html')

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