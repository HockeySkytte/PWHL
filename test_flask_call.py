import sys
sys.path.insert(0, '.')
from report_data import report_store

# Simulate Flask API call
params = {
    'team': 'Minnesota Frost',
    'perspective': 'For',
    'games': '210',  # Flask sends this as a string
}

print("=== Testing Flask-style API call ===")
result = report_store.compute_kpis(**params)
print(f"CF: {result['metrics']['CF']}")
print(f"CA: {result['metrics']['CA']}")
print(f"FF: {result['metrics']['FF']}")
print(f"FA: {result['metrics']['FA']}")

# Manual verification
game210 = [r for r in report_store.rows if r['game_id'] == '210']
print(f"\n=== Manual count ===")
print(f"Total game 210 rows: {len(game210)}")
minn_cf = [r for r in game210 if r['team_for'] == 'Minnesota Frost' and r['is_corsi']]
print(f"Minnesota CF (team_for): {len(minn_cf)}")
minn_ca = [r for r in game210 if r['team_against'] == 'Minnesota Frost' and r['is_corsi']]
print(f"Minnesota CA (team_against): {len(minn_ca)}")
