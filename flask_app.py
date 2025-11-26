
from flask import Flask, render_template, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import requests
import json
import pandas as pd
from datetime import datetime
import csv
import os

app = Flask(__name__)
CORS(app)

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
    date_from = request.args.get('date_from','')
    date_to = request.args.get('date_to','')
    # Apply filters analogous to shotmap
    if seasons_multi:
        rows = [r for r in rows if r['season'] in seasons_multi]
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