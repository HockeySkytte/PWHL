#!/usr/bin/env python3

import json

# Simulate the expected data structure based on your Power Query screenshot
sample_game_data = {
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

def test_processing():
    """Test our data processing logic"""
    from flask_app import PWHLDataAPI
    
    api = PWHLDataAPI()
    processed = api.process_game_summary_data(sample_game_data)
    
    print("=== PROCESSED DATA ===")
    print(json.dumps(processed, indent=2))
    
    # Test specific fields
    if 'homeTeam' in processed:
        print(f"\nHome Team: {processed['homeTeam']['name']}")
        print(f"Home Goalies: {len(processed['homeTeam']['goalies'])}")
        print(f"Home Skaters: {len(processed['homeTeam']['skaters'])}")
        
        if processed['homeTeam']['goalies']:
            goalie = processed['homeTeam']['goalies'][0]
            print(f"First goalie: #{goalie['jersey']} {goalie['name']} ({goalie['position']})")
            
        if processed['homeTeam']['skaters']:
            skater = processed['homeTeam']['skaters'][0]
            print(f"First skater: #{skater['jersey']} {skater['name']} ({skater['position']})")
    
    if 'visitingTeam' in processed:
        print(f"\nVisiting Team: {processed['visitingTeam']['name']}")
        print(f"Visiting Goalies: {len(processed['visitingTeam']['goalies'])}")
        print(f"Visiting Skaters: {len(processed['visitingTeam']['skaters'])}")

if __name__ == "__main__":
    test_processing()