#!/usr/bin/env python3

import requests
import json

def test_game_summary_api():
    """Test the PWHL game summary API to understand the structure"""
    api_base_url = "https://lscluster.hockeytech.com/feed/index.php"
    api_key = "f322673b6bcae299"
    client_code = "pwhl"
    
    params = {
        'feed': 'statviewfeed',
        'view': 'gameSummary',
        'game_id': 105,
        'key': api_key,
        'site_id': 0,
        'client_code': client_code,
        'lang': 'en',
        'league_id': ''
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        print(f"Making request to: {api_base_url}")
        print(f"With params: {params}")
        
        response = requests.get(api_base_url, params=params, headers=headers)
        response.raise_for_status()
        
        print(f"Response status: {response.status_code}")
        print(f"Response content length: {len(response.text)}")
        print(f"First 200 chars of response: {response.text[:200]}")
        
        # Clean the response (remove parentheses)
        raw_data = response.text.strip()
        if raw_data.startswith('(') and raw_data.endswith(')'):
            raw_data = raw_data[1:-1]
        
        if not raw_data:
            print("ERROR: Empty response after cleaning")
            return
            
        data = json.loads(raw_data)
        
        print("=== GAME SUMMARY API STRUCTURE ===")
        print(f"Top-level keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        
        # Look for team data
        if isinstance(data, dict):
            for key, value in data.items():
                print(f"\nKey: {key}")
                if isinstance(value, dict):
                    print(f"  Sub-keys: {list(value.keys())}")
                    # Look for lineup/roster data
                    if 'goalies' in value or 'skaters' in value:
                        print(f"  ** FOUND LINEUP DATA in {key} **")
                        if 'goalies' in value:
                            print(f"    Goalies: {len(value['goalies']) if isinstance(value['goalies'], list) else 'Not a list'}")
                        if 'skaters' in value:
                            print(f"    Skaters: {len(value['skaters']) if isinstance(value['skaters'], list) else 'Not a list'}")
                elif isinstance(value, list) and len(value) > 0:
                    print(f"  List with {len(value)} items")
                    if isinstance(value[0], dict):
                        print(f"    First item keys: {list(value[0].keys())}")
        
        # Look specifically for homeTeam and visitingTeam
        if 'homeTeam' in data:
            print(f"\n** FOUND homeTeam **")
            home_team = data['homeTeam']
            print(f"homeTeam keys: {list(home_team.keys()) if isinstance(home_team, dict) else 'Not a dict'}")
            
        if 'visitingTeam' in data:
            print(f"\n** FOUND visitingTeam **")
            visiting_team = data['visitingTeam']
            print(f"visitingTeam keys: {list(visiting_team.keys()) if isinstance(visiting_team, dict) else 'Not a dict'}")
        
        # Output first few lines of raw JSON for inspection
        print(f"\n=== FIRST 2000 CHARS OF RAW JSON ===")
        print(json.dumps(data, indent=2)[:2000])
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_game_summary_api()