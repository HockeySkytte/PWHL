from flask import Flask, render_template, jsonify, request, send_from_directory, Response, redirect
from flask_cors import CORS
import requests, json, pandas as pd
from datetime import datetime
import csv, os

app = Flask(__name__)
CORS(app)

class PWHLDataAPI:
    def __init__(self):
        self.api_base_url = "https://lscluster.hockeytech.com/feed/index.php"
        self.api_key = "446521baf8c38984"
        self.client_code = "pwhl"
        self.season_mapping = {
            "2023/2024": {"Regular Season": 1, "Playoffs": 3},
            "2024/2025": {"Regular Season": 5, "Playoffs": 6},
            "2025/2026": {"Regular Season": 8}
        }
        self.all_seasons = [1, 3, 5, 6, 8]
        self.season_years = ["2023/2024", "2024/2025", "2025/2026"]
        self.season_states = ["Regular Season", "Playoffs"]
        self.teams = self.load_team_data()

    def load_team_data(self):
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
                    teams[row['name']] = team_data
                    if row['name'].startswith('New York'):
                        city_name = 'New York'
                    elif row['name'].startswith('Montréal'):
                        city_to_full_name['Montreal'] = row['name']
                        city_to_full_name['Montréal'] = row['name']
                        city_name = 'Montréal'
                    else:
                        parts = row['name'].split(' ')
                        if parts[0] == 'PWHL' and len(parts) > 1:
                            city_name = parts[1]
                        else:
                            city_name = parts[0]
                    city_to_full_name[city_name] = row['name']
        except Exception as e:
            print(f"Error loading team data: {e}")
        self.city_to_full_name = city_to_full_name
        return teams

    # ---------------- Schedule / Game Methods ---------------- #
    def fetch_schedule_data(self, season):
        params = {
            'feed': 'statviewfeed', 'view': 'schedule', 'team': -1, 'season': season,
            'month': -1, 'location': 'homeaway', 'key': self.api_key, 'client_code': self.client_code,
            'site_id': 0, 'league_id': 1, 'conference_id': -1, 'division_id': -1, 'lang': 'en'
        }
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            r = requests.get(self.api_base_url, params=params, headers=headers)
            r.raise_for_status()
            raw = r.text.strip()
            if raw.startswith('(') and raw.endswith(')'):
                raw = raw[1:-1]
            data = json.loads(raw)
            if isinstance(data, list) and data and 'sections' in data[0]:
                sections = data[0]['sections']
                if sections and 'data' in sections[0]:
                    return sections[0]['data']
            return []
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []

    def parse_games_data(self, games_data, season):
        parsed = []
        for game in games_data:
            row = game.get('row', {})
            season_state = "Regular Season" if season in [1,5,8] else ("Playoffs" if season in [3,6] else "Regular Season")
            if season in [1,3]: season_year = "2023/2024"
            elif season in [5,6]: season_year = "2024/2025"
            elif season in [8]: season_year = "2025/2026"
            else: season_year = "Unknown"
            date_str = row.get('date_with_day', '')
            try:
                if date_str:
                    if season_year == "2023/2024":
                        year = 2024 if any(m in date_str for m in ["Jan","Feb","Mar","Apr","May"]) else 2023
                    elif season_year == "2024/2025":
                        year = 2025 if any(m in date_str for m in ["Jan","Feb","Mar","Apr","May"]) else 2024
                    elif season_year == "2025/2026":
                        year = 2026 if any(m in date_str for m in ["Jan","Feb","Mar","Apr","May"]) else 2025
                    else:
                        year = datetime.now().year
                    date_parsed = pd.to_datetime(f"{date_str}, {year}", format='%a, %b %d, %Y', errors='coerce')
                    formatted_date = date_parsed.strftime('%a, %b %d') if pd.notna(date_parsed) else 'TBD'
                    full_date = date_parsed.strftime('%Y-%m-%d') if pd.notna(date_parsed) else ''
                else:
                    formatted_date = 'TBD'; full_date=''; date_parsed = pd.NaT
            except Exception:
                formatted_date = 'TBD'; full_date=''; date_parsed = pd.NaT
            away_city = row.get('visiting_team_city','')
            home_city = row.get('home_team_city','')
            away_full = self.city_to_full_name.get(away_city, away_city)
            home_full = self.city_to_full_name.get(home_city, home_city)
            away_logo = self.teams.get(away_full, {}).get('logo','')
            home_logo = self.teams.get(home_full, {}).get('logo','')
            away_id = row.get('visiting_team_id','') or str(self.teams.get(away_full,{}).get('id') or '')
            home_id = row.get('home_team_id','') or str(self.teams.get(home_full,{}).get('id') or '')
            parsed.append({
                'date': formatted_date,'full_date': full_date,'date_obj': date_parsed,
                'season_year': season_year,'season_state': season_state,
                'away_team': away_full,'home_team': home_full,
                'away_team_id': away_id,'home_team_id': home_id,
                'away_team_city': away_city,'home_team_city': home_city,
                'away_team_logo': away_logo,'home_team_logo': home_logo,
                'status': row.get('game_status',''),
                'away_score': row.get('visiting_goal_count',''),
                'home_score': row.get('home_goal_count',''),
                'game_id': row.get('game_id',''),
                'venue': row.get('venue_name','')
            })
        return parsed

    def fetch_game_summary(self, game_id):
        params = {
            'feed':'statviewfeed','view':'gameSummary','game_id':game_id,'key':self.api_key,
            'site_id':0,'client_code':self.client_code,'lang':'en','league_id': ''
        }
        headers={'User-Agent':'Mozilla/5.0'}
        try:
            r = requests.get(self.api_base_url, params=params, headers=headers)
            r.raise_for_status()
            raw = r.text.strip()
            if raw.startswith('(') and raw.endswith(')'):
                raw = raw[1:-1]
            data = json.loads(raw)
            return self.process_game_summary_data(data)
        except Exception as e:
            print(f"Error fetching game summary for game {game_id}: {e}")
            return None

    def process_game_summary_data(self, data):
        if not isinstance(data, dict):
            return data
        processed = {}
        if isinstance(data.get('homeTeam'), dict):
            home = data['homeTeam']
            processed['homeTeam'] = {'name': home.get('name','Home Team'), 'goalies': [], 'skaters': []}
            for g in home.get('goalies', []) or []:
                eg = self.expand_player_data(g)
                if eg: processed['homeTeam']['goalies'].append(eg)
            for s in home.get('skaters', []) or []:
                es = self.expand_player_data(s)
                if es: processed['homeTeam']['skaters'].append(es)
        if isinstance(data.get('visitingTeam'), dict):
            vis = data['visitingTeam']
            processed['visitingTeam'] = {'name': vis.get('name','Visiting Team'), 'goalies': [], 'skaters': []}
            for g in vis.get('goalies', []) or []:
                eg = self.expand_player_data(g)
                if eg: processed['visitingTeam']['goalies'].append(eg)
            for s in vis.get('skaters', []) or []:
                es = self.expand_player_data(s)
                if es: processed['visitingTeam']['skaters'].append(es)
        return processed

    def expand_player_data(self, player):
        if not isinstance(player, dict):
            return None
        info = player.get('info') if isinstance(player.get('info'), dict) else {}
        expanded = {}
        if info:
            expanded.update({
                'name': f"{info.get('firstName','')} {info.get('lastName','')}".strip(),
                'jersey': info.get('jerseyNumber',''),
                'position': info.get('position',''),
                'birthDate': info.get('birthDate',''),
                'playerImageURL': info.get('playerImageURL','')
            })
        stats = player.get('stats') if isinstance(player.get('stats'), dict) else None
        if stats:
            expanded['stats'] = stats
        return expanded or None

    def fetch_play_by_play(self, game_id):
        params = {
            'feed':'statviewfeed','view':'gameCenterPlayByPlay','game_id':game_id,'key':self.api_key,
            'site_id':0,'client_code':self.client_code,'lang':'en','league_id': ''
        }
        headers={'User-Agent':'Mozilla/5.0'}
        try:
            r = requests.get(self.api_base_url, params=params, headers=headers)
            r.raise_for_status()
            raw = r.text.strip()
            if raw.startswith('(') and raw.endswith(')'):
                raw = raw[1:-1]
            return json.loads(raw)
        except Exception as e:
            print(f"Error fetching play-by-play for game {game_id}: {e}")
            return None

# Initialize the data API
data_api = PWHLDataAPI()
from export_utils import generate_lineups_csv, generate_pbp_csv

@app.route('/data/<path:filename>')
def serve_data_file(filename: str):
    # Only allow serving from Data directory and restrict to .csv
    safe_root = os.path.join(app.root_path, 'Data')
    abs_path = os.path.abspath(os.path.join(safe_root, filename))
    if not abs_path.startswith(os.path.abspath(safe_root)):
        return jsonify({'error':'Forbidden'}), 403
    if not abs_path.lower().endswith('.csv'):
        return jsonify({'error':'Unsupported file type'}), 400
    rel_dir = os.path.dirname(os.path.relpath(abs_path, safe_root))
    dir_to_send = os.path.join(safe_root, rel_dir)
    fname = os.path.basename(abs_path)
    if not os.path.exists(abs_path):
        return jsonify({'error':'Not found'}), 404
    return send_from_directory(dir_to_send, fname, mimetype='text/csv')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/report')
def report_page():
    # Serve the static report HTML (no API dependencies for data)
    return render_template('report/report.html')

@app.route('/Report')
def report_page_cap():
    # Provide capitalized path alias expected by user
    return redirect('/report', code=302)

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