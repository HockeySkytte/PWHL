import requests
import json

# Test the game detail APIs
base_url = "http://localhost:8501"

def test_game_summary(game_id):
    """Test game summary API"""
    try:
        response = requests.get(f"{base_url}/api/game/summary/{game_id}")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Game {game_id} summary fetched successfully")
            print(f"  Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            return data
        else:
            print(f"✗ Game {game_id} summary failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ Error fetching game {game_id} summary: {e}")
        return None

def test_play_by_play(game_id):
    """Test play-by-play API"""
    try:
        response = requests.get(f"{base_url}/api/game/playbyplay/{game_id}")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Game {game_id} play-by-play fetched successfully")
            print(f"  Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            return data
        else:
            print(f"✗ Game {game_id} play-by-play failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ Error fetching game {game_id} play-by-play: {e}")
        return None

if __name__ == "__main__":
    print("Testing PWHL Game Detail APIs...")
    print("=" * 50)
    
    # Test with a few different game IDs
    test_games = [105, 106, 107, 2]  # Using some game IDs we know exist
    
    for game_id in test_games:
        print(f"\nTesting Game ID: {game_id}")
        print("-" * 30)
        
        # Test summary
        summary_data = test_game_summary(game_id)
        
        # Test play-by-play
        pbp_data = test_play_by_play(game_id)
        
        if summary_data and isinstance(summary_data, dict):
            # Show a sample of the summary data structure
            print(f"  Summary data sample (first 200 chars): {str(summary_data)[:200]}...")
        
        if pbp_data and isinstance(pbp_data, dict):
            # Show a sample of the play-by-play data structure
            print(f"  Play-by-play data sample (first 200 chars): {str(pbp_data)[:200]}...")
    
    print("\n" + "=" * 50)
    print("API testing complete!")