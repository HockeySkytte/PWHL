#!/usr/bin/env python3

import requests
import json
from bs4 import BeautifulSoup

def test_pwhl_game_urls():
    """Test the PWHL game URLs to understand their structure"""
    
    # Test the lineup URL format you mentioned
    game_id = 105
    lineup_url = f"https://www.thepwhlhub.com/game/{game_id}/"
    
    print(f"Testing lineup URL: {lineup_url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(lineup_url, headers=headers)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            # Look for JSON data in the page
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for script tags that might contain lineup data
            scripts = soup.find_all('script')
            for i, script in enumerate(scripts):
                if script.string and ('goalies' in script.string.lower() or 'skaters' in script.string.lower() or 'lineup' in script.string.lower()):
                    print(f"\n=== SCRIPT {i} WITH LINEUP DATA ===")
                    print(script.string[:500])
                    
            # Look for specific data structures
            if 'homeTeam' in response.text:
                print("\n** FOUND 'homeTeam' in page content **")
            if 'visitingTeam' in response.text:
                print("\n** FOUND 'visitingTeam' in page content **")
            if 'goalies' in response.text:
                print("\n** FOUND 'goalies' in page content **")
            if 'skaters' in response.text:
                print("\n** FOUND 'skaters' in page content **")
                
        else:
            print(f"Error: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Test the alternative API approach with different parameters
    print("\n\n=== TESTING ALTERNATIVE API PARAMETERS ===")
    api_base_url = "https://lscluster.hockeytech.com/feed/index.php"
    
    # Try different parameter combinations
    param_sets = [
        {
            'feed': 'statviewfeed',
            'view': 'gameSummary',
            'game_id': game_id,
            'key': 'f322673b6bcae299',
            'site_id': 1,  # Try different site_id
            'client_code': 'pwhl',
            'lang': 'en'
        },
        {
            'feed': 'modulekit',
            'view': 'gameCenterPlayByPlay',
            'game_id': game_id,
            'key': 'f322673b6bcae299',
            'site_id': 0,
            'client_code': 'pwhl',
            'lang': 'en'
        }
    ]
    
    for i, params in enumerate(param_sets):
        print(f"\nTrying parameter set {i+1}: {params}")
        try:
            response = requests.get(api_base_url, params=params, headers=headers)
            print(f"Status: {response.status_code}, Length: {len(response.text)}")
            print(f"First 100 chars: {response.text[:100]}")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_pwhl_game_urls()