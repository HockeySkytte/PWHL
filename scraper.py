import requests
import json
import pandas as pd
from datetime import datetime
import os
from typing import Dict, List, Optional


class PWHLScraper:
    """PWHL API scraper for schedule and game data."""
    
    BASE_URL = "https://lscluster.hockeytech.com/feed/index.php"
    API_KEY = "446521baf8c38984"
    CLIENT_CODE = "pwhl"
    
    # Season mapping
    SEASONS = {
        1: "2023/2024 Regular Season",
        3: "2023/2024 Playoffs",
        5: "2024/2025 Regular Season", 
        6: "2024/2025 Playoffs"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_schedule(self, season: int = 5, team: int = -1, month: int = -1) -> Optional[Dict]:
        """
        Fetch schedule data from PWHL API.
        
        Args:
            season: Season number (1=2023/24 regular, 3=2023/24 playoffs, 
                   5=2024/25 regular, 6=2024/25 playoffs)
            team: Team ID (-1 for all teams)
            month: Month (-1 for all months)
            
        Returns:
            Dictionary containing schedule data, or None if error
        """
        params = {
            'feed': 'statviewfeed',
            'view': 'schedule',
            'team': team,
            'season': season,
            'month': month,
            'location': 'homeaway',
            'key': self.API_KEY,
            'client_code': self.CLIENT_CODE,
            'site_id': 0,
            'league_id': 1,
            'conference_id': -1,
            'division_id': -1,
            'lang': 'en'
        }
        
        try:
            print(f"Fetching schedule for {self.SEASONS.get(season, f'Season {season}')}...")
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            # The API returns JSON wrapped in parentheses, so we need to clean it
            raw_data = response.text.strip()
            if raw_data.startswith('(') and raw_data.endswith(')'):
                raw_data = raw_data[1:-1]
            
            data = json.loads(raw_data)
            
            # Handle the actual API response structure - it's a list with sections
            if isinstance(data, list) and len(data) > 0 and 'sections' in data[0]:
                sections = data[0]['sections']
                if sections and len(sections) > 0 and 'data' in sections[0]:
                    games = sections[0]['data']
                    print(f"Successfully fetched {len(games)} games")
                else:
                    print("No game data found in sections")
                    games = []
            else:
                # Fallback to original structure check
                if isinstance(data, dict) and 'SiteKit' in data and 'Schedule' in data['SiteKit']:
                    games = data['SiteKit']['Schedule']
                    print(f"Successfully fetched {len(games)} games")
                else:
                    print("Unknown data structure")
                    games = []
            
            # Return in a consistent format
            return {
                'raw_data': data,
                'games': games
            }
            
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None
    
    def save_schedule_data(self, data: Dict, season: int, filename: Optional[str] = None) -> str:
        """
        Save schedule data to JSON file.
        
        Args:
            data: Schedule data dictionary
            season: Season number
            filename: Optional custom filename
            
        Returns:
            Path to saved file
        """
        if filename is None:
            season_name = self.SEASONS.get(season, f"season_{season}")
            safe_name = season_name.replace("/", "_").replace(" ", "_").lower()
            filename = f"pwhl_schedule_{safe_name}.json"
        
        filepath = os.path.join(os.getcwd(), filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Schedule data saved to: {filepath}")
        return filepath
    
    def parse_schedule_to_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Parse schedule data into a pandas DataFrame for analysis.
        
        Args:
            data: Processed schedule data from API
            
        Returns:
            DataFrame with game information
        """
        games = data.get('games', [])
        
        parsed_games = []
        for game in games:
            # Extract the row data which contains the actual game information
            row = game.get('row', {})
            
            game_info = {
                'game_id': row.get('game_id'),
                'date': row.get('date_with_day'),
                'time': row.get('game_time'),
                'home_team': row.get('home_team_city'),
                'away_team': row.get('visiting_team_city'),
                'home_team_name': row.get('home_team_city'),
                'away_team_name': row.get('visiting_team_city'),
                'home_score': row.get('home_goal_count'),
                'away_score': row.get('visiting_goal_count'),
                'status': row.get('game_status'),
                'attendance': row.get('attendance'),
                'venue': row.get('venue_name'),
                'season_type': None,  # Not available in this format
                'game_number': row.get('game_id')
            }
            parsed_games.append(game_info)
        
        df = pd.DataFrame(parsed_games)
        
        # Convert date column to datetime - handle the "Sat, Nov 30" format
        if 'date' in df.columns and not df.empty:
            # The date is in format like "Sat, Nov 30" without year, so we need to add year
            current_year = datetime.now().year
            df['date_parsed'] = df['date'].apply(lambda x: f"{x}, {current_year}" if pd.notna(x) and x else x)
            df['date'] = pd.to_datetime(df['date_parsed'], format='%a, %b %d, %Y', errors='coerce')
        
        return df


def main():
    """Main function to demonstrate the scraper."""
    scraper = PWHLScraper()
    
    # Fetch current season schedule (2024/2025 regular season)
    current_season_data = scraper.get_schedule(season=5)
    
    if current_season_data:
        # Save the raw data
        scraper.save_schedule_data(current_season_data, season=5)
        
        # Parse to DataFrame for analysis
        df = scraper.parse_schedule_to_dataframe(current_season_data)
        print(f"\nSchedule DataFrame shape: {df.shape}")
        print("\nFirst few games:")
        print(df.head())
        
        # Save DataFrame as CSV
        csv_filename = "pwhl_schedule_2024_2025_regular.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\nSchedule CSV saved to: {csv_filename}")
    
    # Optionally fetch other seasons
    print("\n" + "="*50)
    print("Available seasons:")
    for season_id, season_name in scraper.SEASONS.items():
        print(f"Season {season_id}: {season_name}")


if __name__ == "__main__":
    main()