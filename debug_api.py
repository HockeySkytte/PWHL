import requests
import json

# Test the PWHL API to understand the response structure
url = "https://lscluster.hockeytech.com/feed/index.php"
params = {
    'feed': 'statviewfeed',
    'view': 'schedule',
    'team': -1,
    'season': 5,
    'month': -1,
    'location': 'homeaway',
    'key': '446521baf8c38984',
    'client_code': 'pwhl',
    'site_id': 0,
    'league_id': 1,
    'conference_id': -1,
    'division_id': -1,
    'lang': 'en'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

print("Testing PWHL API...")
response = requests.get(url, params=params, headers=headers)
print(f"Status code: {response.status_code}")
print(f"Response length: {len(response.text)} characters")

# Show first 500 characters
print(f"\nFirst 500 characters:")
print(response.text[:500])

# Try to parse JSON
raw_text = response.text.strip()
if raw_text.startswith('(') and raw_text.endswith(')'):
    raw_text = raw_text[1:-1]
    print("\nRemoved parentheses")

try:
    data = json.loads(raw_text)
    print(f"\nJSON parsed successfully!")
    print(f"Data type: {type(data)}")
    
    if isinstance(data, dict):
        print(f"Dictionary keys: {list(data.keys())}")
    elif isinstance(data, list):
        print(f"List length: {len(data)}")
        if data:
            print(f"First item type: {type(data[0])}")
            if isinstance(data[0], dict):
                print(f"First item keys: {list(data[0].keys())}")
                
                # Explore sections
                if 'sections' in data[0]:
                    sections = data[0]['sections']
                    print(f"Number of sections: {len(sections)}")
                    
                    if sections:
                        first_section = sections[0]
                        print(f"First section keys: {list(first_section.keys())}")
                        
                        if 'data' in first_section:
                            games = first_section['data']
                            print(f"Number of games: {len(games)}")
                            
                            if games:
                                print(f"First game type: {type(games[0])}")
                                if isinstance(games[0], dict):
                                    print(f"First game keys: {list(games[0].keys())}")
                                    print(f"\nFirst game sample:")
                                    for key, value in list(games[0].items())[:10]:  # First 10 fields
                                        print(f"  {key}: {value}")
                                        
except json.JSONDecodeError as e:
    print(f"JSON parse error: {e}")
    print(f"Error position: {e.pos}")
    print(f"Text around error: {raw_text[max(0, e.pos-50):e.pos+50]}")