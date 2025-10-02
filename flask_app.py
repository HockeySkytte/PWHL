from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import json
import pandas as pd
from datetime import datetime
import csv
import os
import os

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
            
            game_info = {
                'date': formatted_date,
                'full_date': full_date,
                'date_obj': date_parsed,
                'season_year': season_year,
                'season_state': season_state,
                'away_team': away_team_full_name,  # Use full team name
                'home_team': home_team_full_name,  # Use full team name
                'away_team_id': row.get('visiting_team_id', ''),
                'home_team_id': row.get('home_team_id', ''),
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

@app.route('/')
def index():
    return render_template('index.html')

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