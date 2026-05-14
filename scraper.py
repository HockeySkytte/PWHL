import requests
import json
import pandas as pd
from datetime import datetime
import os
import re
from typing import Dict, List, Optional


class PWHLScraper:
    """PWHL API scraper for schedule and game data."""
    
    BASE_URL = "https://lscluster.hockeytech.com/feed/index.php"
    API_KEY = "446521baf8c38984"
    CLIENT_CODE = "pwhl"
    
    FALLBACK_SEASONS = {
        1: "2023/2024 Regular Season",
        3: "2023/2024 Playoffs",
        5: "2024/2025 Regular Season",
        6: "2024/2025 Playoffs",
        8: "2025/2026 Regular Season",
        9: "2025/2026 Playoffs",
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.seasons = self._fetch_seasons()

    def _fetch_seasons(self) -> Dict[int, str]:
        """Fetch live season metadata and fall back to a minimal known set."""
        params = {
            'feed': 'modulekit',
            'view': 'seasons',
            'key': self.API_KEY,
            'client_code': self.CLIENT_CODE,
            'site_id': 0,
            'lang': 'en',
        }
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            raw_data = response.text.strip()
            if raw_data.startswith('(') and raw_data.endswith(')'):
                raw_data = raw_data[1:-1]
            data = json.loads(raw_data)
            season_rows = data.get('SiteKit', {}).get('Seasons', [])
            seasons: Dict[int, str] = {}
            for row in season_rows:
                sid = int(row.get('season_id'))
                name = (row.get('season_name') or '').strip()
                if not name:
                    continue
                seasons[sid] = self._normalize_season_name(name, str(row.get('playoff', '0')) == '1')
            if seasons:
                return dict(sorted(seasons.items()))
        except Exception as exc:
            print(f"Warning: could not fetch seasons from API ({exc}); using fallback")
        return dict(self.FALLBACK_SEASONS)

    @staticmethod
    def _normalize_season_name(name: str, is_playoff: bool) -> str:
        lower = name.lower()
        if 'preseason' in lower:
            suffix = 'Preseason'
        elif is_playoff or 'playoff' in lower:
            suffix = 'Playoffs'
        else:
            suffix = 'Regular Season'

        match = re.search(r'(\d{4})-(\d{2})', name)
        if match:
            start_year = int(match.group(1))
            end_year = start_year // 100 * 100 + int(match.group(2))
            return f"{start_year}/{end_year} {suffix}"

        match = re.search(r'(\d{4})', name)
        if match:
            end_year = int(match.group(1))
            return f"{end_year - 1}/{end_year} {suffix}"

        return name
    
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
            print(f"Fetching schedule for {self.seasons.get(season, f'Season {season}')}...")
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
            season_name = self.seasons.get(season, f"season_{season}")
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
    
    regular_seasons = [sid for sid, name in scraper.seasons.items() if 'Regular Season' in name]
    default_season = max(regular_seasons) if regular_seasons else 5

    # Fetch the most recent regular-season schedule
    current_season_data = scraper.get_schedule(season=default_season)
    
    if current_season_data:
        # Save the raw data
        scraper.save_schedule_data(current_season_data, season=default_season)
        
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
    for season_id, season_name in scraper.seasons.items():
        print(f"Season {season_id}: {season_name}")


if __name__ == "__main__":
    main()