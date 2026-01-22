import os
import csv
import json
import time
import re
import argparse
from typing import Dict, Any, List, Tuple

import requests
import sys

# Discover repo root (script is in scripts/) early so we can add path before imports
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from export_utils import generate_lineups_csv, generate_pbp_csv

BASE_URL = os.environ.get("PWHL_BASE_URL", "http://localhost:8501")
TEAMS_CSV = os.path.join(REPO_ROOT, 'Teams.csv')
OUT_LINEUPS = os.path.join("Data", "Lineups")
OUT_PBP = os.path.join("Data", "Play-by-Play")

# Ensure output directories exist
os.makedirs(OUT_LINEUPS, exist_ok=True)
os.makedirs(OUT_PBP, exist_ok=True)

# Load teams to get official color and ensure consistent naming
def load_teams(path: str) -> Dict[str, Dict[str, str]]:
    teams: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(path):
        return teams
    with open(path, 'r', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            name = (row.get('name') or '').strip()
            if not name:
                continue
            tid = (row.get('id') or row.get('\ufeffid') or '').strip()
            team_code = (row.get('team_code') or row.get('code') or '').strip()
            teams[name] = {
                'id': tid,
                'color': row.get('color', ''),
                'logo': row.get('logo', ''),
                'nickname': row.get('nickname', ''),
                'team_code': team_code,
            }
    return teams

TEAM_MAP = load_teams(TEAMS_CSV)
TEAM_COLOR_BY_NAME = { name: (info.get('color') or '') for name, info in TEAM_MAP.items() }
TEAM_COLOR_BY_ID = { str(info.get('id') or ''): (info.get('color') or '') for info in TEAM_MAP.values() }

# Helper: safe get

def jget(d: Dict[str, Any], *path, default=""):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def fetch_json(url: str) -> Any:
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError as e:
        raise SystemExit(
            f"Cannot connect to {url}. Make sure your Flask app is running (python flask_app.py) "
            f"or set PWHL_BASE_URL to your deployed URL.\nOriginal error: {e}"
        )


def normalize_game_date(date_label: str, season_year: str, full_date: str = "") -> str:
    # Prefer server-provided ISO date if available
    if full_date and re.match(r"^\d{4}-\d{2}-\d{2}$", full_date):
        return full_date
    # Compute from season and month/day label (e.g., 'Wed, Oct 12')
    if not date_label or not season_year or '/' not in season_year:
        return str(full_date or date_label or '')
    m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})", str(date_label), re.I)
    if not m:
        return str(full_date or date_label or '')
    mon_abbr = m.group(1).lower()
    day = int(m.group(2))
    month_map = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    mon = month_map.get(mon_abbr[:3], 0)
    try:
        y1, y2 = map(int, season_year.split('/'))
    except Exception:
        return str(full_date or date_label or '')
    # If month is <= July, use last year; else use first year
    year = y2 if mon and mon <= 7 else y1
    if not mon:
        return str(full_date or date_label or '')
    return f"{year:04d}-{mon:02d}-{day:02d}"


def get_seasons(base_url: str) -> List[str]:
    # Query schedule with All to get all games across seasons and infer season_years present in server
    url = f"{base_url}/api/schedule?season_year=All&season_state=All&team=All&status=All"
    data = fetch_json(url)
    seasons = set()
    for g in data.get("games", []):
        sy = g.get("season_year")
        if sy:
            seasons.add(sy)
    return sorted(seasons)


def list_all_games(base_url: str) -> List[Dict[str, Any]]:
    # Fetch across all seasons
    url = f"{base_url}/api/schedule?season_year=All&season_state=All&team=All&status=All"
    data = fetch_json(url)
    return data.get("games", [])


def expand_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    # Already expanded by server; just ensure structure
    return summary or {}


def get_team_color(team_name: str, team_id: str = '') -> str:
    # Prefer exact name match from Teams.csv; fallback to id mapping
    if team_name and team_name in TEAM_COLOR_BY_NAME:
        return TEAM_COLOR_BY_NAME[team_name]
    tid = str(team_id or '').strip()
    if tid and tid in TEAM_COLOR_BY_ID:
        return TEAM_COLOR_BY_ID[tid]
    return ''


def escape_csv(val: Any) -> str:
    s = "" if val is None else str(val)
    if any(c in s for c in [',', '"', '\n']):
        return '"' + s.replace('"', '""') + '"'
    return s


def toi_to_seconds(toi_val: Any) -> Any:
    if toi_val is None:
        return ""
    if isinstance(toi_val, (int, float)):
        return int(max(0, int(toi_val)))
    s = str(toi_val).strip()
    if not s:
        return ""
    parts = s.split(':')
    try:
        nums = list(map(int, parts))
    except Exception:
        try:
            return int(s)
        except Exception:
            return ""
    if len(nums) == 3:
        h, m, sec = nums
        return h * 3600 + m * 60 + sec
    if len(nums) == 2:
        m, sec = nums
        return m * 60 + sec
    return nums[0]


def build_teams_meta() -> Dict[str, Dict[str, str]]:
    """Construct name/code mapping for generate_pbp_csv from Teams.csv."""
    name_to_code: Dict[str, str] = {}
    code_to_name: Dict[str, str] = {}
    for name, info in TEAM_MAP.items():
        code = str(info.get('team_code') or info.get('code') or '').strip()
        if code:
            name_to_code[name] = code
            code_to_name[code] = name
    return { 'name_to_code': name_to_code, 'code_to_name': code_to_name }

def infer_missing_team_ids(game: Dict[str, Any], events: List[Dict[str, Any]] | None) -> None:
    """If schedule game dict is missing home/away numeric ids, attempt to infer them from PBP events.
    Logic:
      1. Collect (teamId, abbreviation) pairs from details.team / details.againstTeam.
      2. Build abbreviation->teamId mapping (first occurrence wins).
      3. For each of home_team / away_team, look up team_code in TEAM_MAP; if code matches an abbreviation we found, assign that id.
      4. If still missing and exactly two distinct teamIds collected, assign deterministically: lower id -> whichever name sorts first to keep stable, unless one id already assigned.
    Mutates game in-place.
    """
    if not events:
        return
    home_id = str(game.get('home_team_id') or '').strip()
    away_id = str(game.get('away_team_id') or '').strip()
    if home_id and away_id:
        return
    id_to_abbr: Dict[str, str] = {}
    abbr_to_id: Dict[str, str] = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue
        d = ev.get('details') or {}
        for key in ('team','againstTeam'):
            t = d.get(key)
            if isinstance(t, dict):
                tid = str(t.get('id') or '').strip()
                ab = str(t.get('abbreviation') or '').strip().upper()
                if tid and ab:
                    id_to_abbr.setdefault(tid, ab)
                    abbr_to_id.setdefault(ab, tid)
        # shooterTeamId fallback
        stid = d.get('shooterTeamId')
        if stid is not None:
            stid_s = str(stid)
            if stid_s and stid_s not in id_to_abbr:
                # can't know abbreviation here; keep placeholder
                id_to_abbr.setdefault(stid_s, '')
    # Map schedule names via team_code
    def team_code_for(name: str) -> str:
        info = TEAM_MAP.get(name)
        return (info.get('team_code') if info else '') or ''
    home_name = game.get('home_team') or ''
    away_name = game.get('away_team') or ''
    home_code = team_code_for(home_name).upper()
    away_code = team_code_for(away_name).upper()
    if not home_id and home_code and home_code in abbr_to_id:
        home_id = abbr_to_id[home_code]
    if not away_id and away_code and away_code in abbr_to_id:
        away_id = abbr_to_id[away_code]
    # If still missing, but we have exactly two ids, assign deterministically
    distinct_ids = sorted([tid for tid in id_to_abbr.keys() if tid])
    distinct_ids = list(dict.fromkeys(distinct_ids))  # dedupe preserve order
    if (not home_id or not away_id) and len(distinct_ids) == 2:
        a, b = distinct_ids
        # If one already set, assign the other to the remaining slot
        if home_id and not away_id and a == home_id:
            away_id = b
        elif home_id and not away_id and b == home_id:
            away_id = a
        elif away_id and not home_id and a == away_id:
            home_id = b
        elif away_id and not home_id and b == away_id:
            home_id = a
        elif not home_id and not away_id:
            # Stable assignment: pick based on lexicographic comparison of team names if possible
            if home_name and away_name:
                # assign smaller id to lexicographically smaller name to stay deterministic
                ids_sorted = sorted(distinct_ids)
                if home_name < away_name:
                    home_id, away_id = ids_sorted[0], ids_sorted[1]
                else:
                    home_id, away_id = ids_sorted[1], ids_sorted[0]
            else:
                home_id, away_id = a, b
    if home_id:
        game['home_team_id'] = home_id
    if away_id:
        game['away_team_id'] = away_id


def prettify_event_label(key: str) -> str:
    m = {
        'goal': 'Goal',
        'shot': 'Shot',
        'penalty': 'Penalty',
        'blocked-shot': 'Block',
        'blocked_shot': 'Block',
        'SO_goal': 'Goal',
        'SO_miss': 'Shot',
    }
    return m.get(key, key.replace('_', ' ').title())


def get_event_key(ev: Dict[str, Any]) -> str:
    if '_overrideEvent' in ev:
        return ev['_overrideEvent']
    return ev.get('event') or ev.get('type') or ''


from typing import Tuple

def csv_escape_list(items: List[Dict[str, Any]], field_no: str, field_name_first: str = 'firstName', field_name_last: str = 'lastName') -> Tuple[str, str]:
    # returns (numbers_joined, names_joined)
    nos: List[str] = []
    names: List[str] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        no = it.get('jerseyNumber') or it.get('id') or ''
        name = ((it.get(field_name_first, '') + ' ' + it.get(field_name_last, '')).strip())
        nos.append(str(no))
        names.append(name)
    return ' '.join(nos), ' | '.join(names)


def convert_x(x: Any) -> str:
    if x is None or x == "":
        return ""
    try:
        xf = float(x)
    except Exception:
        return ""
    return f"{((xf - 300) / 300 * 100):.1f}"


def convert_y(y: Any) -> str:
    if y is None or y == "":
        return ""
    try:
        yf = float(y)
    except Exception:
        return ""
    return f"{((yf - 150) / 150 * 42.5):.1f}"


def fetch_summary_and_pbp(game_id: str) -> Tuple[Dict[str, Any] | None, List[Dict[str, Any]] | None]:
    """Fetch summary and pbp JSON from server endpoints."""
    summary = None
    pbp = None
    try:
        r1 = requests.get(f"{BASE_URL}/api/game/summary/{game_id}", timeout=60)
        if r1.ok:
            summary = r1.json()
    except Exception:
        pass
    try:
        r2 = requests.get(f"{BASE_URL}/api/game/playbyplay/{game_id}", timeout=60)
        if r2.ok:
            data = r2.json()
            if isinstance(data, dict):
                pbp = data.get('events') or data.get('pbp') or []
            elif isinstance(data, list):
                pbp = data
    except Exception:
        pass
    return summary, pbp


def main():
    parser = argparse.ArgumentParser(description='Export PWHL Lineups and PBP CSVs')
    parser.add_argument('--game-id', type=str, help='Export a single game id (e.g., 105). If omitted, exports all games.')
    parser.add_argument('--start-date', type=str, help='Start date for filtering games (YYYY-MM-DD format, e.g., 2025-11-01)')
    parser.add_argument('--end-date', type=str, help='End date for filtering games (YYYY-MM-DD format, e.g., 2025-11-30)')
    parser.add_argument('--season', type=str, help='Filter by season (e.g., 2025/2026)')
    args = parser.parse_args()

    if args.game_id:
        # Fetch that single game by scanning schedule
        print(f"Fetching game {args.game_id} from {BASE_URL}…")
        games = list_all_games(BASE_URL)
        game = next((g for g in games if str(g.get('game_id')) == str(args.game_id)), None)
        if not game:
            raise SystemExit(f"Game {args.game_id} not found in schedule.")
        games_to_process = [game]
    else:
        print(f"Fetching all games from {BASE_URL}…")
        games_to_process = list_all_games(BASE_URL)
        
        # Apply filters
        if args.season:
            print(f"Filtering by season: {args.season}")
            games_to_process = [g for g in games_to_process if g.get('season_year') == args.season]
        
        if args.start_date or args.end_date:
            # Filter by date
            filtered_games = []
            for g in games_to_process:
                # Prefer server-provided ISO date if available (full_date/date_parsed).
                # Fall back to computing from the human-readable label + season_year.
                iso_date = g.get('full_date') or g.get('date_parsed') or g.get('game_date') or ''
                date_label = g.get('date') or g.get('date_label') or ''
                if iso_date or date_label:
                    # Normalize to YYYY-MM-DD if needed
                    date_str = normalize_game_date(
                        date_label,
                        g.get('season_year', ''),
                        iso_date
                    )
                    
                    # Check date range
                    if args.start_date and date_str < args.start_date:
                        continue
                    if args.end_date and date_str > args.end_date:
                        continue
                    
                filtered_games.append(g)
            
            games_to_process = filtered_games
            if args.start_date and args.end_date:
                print(f"Filtering games between {args.start_date} and {args.end_date}")
            elif args.start_date:
                print(f"Filtering games from {args.start_date} onward")
            elif args.end_date:
                print(f"Filtering games up to {args.end_date}")
        
        print(f"Found {len(games_to_process)} games")

    for i, game in enumerate(games_to_process, start=1):
        gid = game.get('game_id')
        if not gid:
            continue
        gid_str = str(gid)
        tag = f"[{i}/{len(games_to_process)}] " if not args.game_id else ""
        print(f"{tag}Game {gid_str}…", end='', flush=True)

        lineups_path = os.path.join(OUT_LINEUPS, f"{gid_str}_teams.csv")
        pbp_path = os.path.join(OUT_PBP, f"{gid_str}_shots.csv")
        summary, pbp = fetch_summary_and_pbp(gid_str)
        # Infer missing numeric team ids before generation (needed for strength orientation & team mapping)
        infer_missing_team_ids(game, pbp)
        teams_meta = build_teams_meta()

        wrote_any = False
        if summary:
            try:
                csv_text = generate_lineups_csv(game, summary, TEAM_COLOR_BY_NAME, TEAM_COLOR_BY_ID)
                if csv_text.strip():
                    with open(lineups_path, 'w', encoding='utf-8', newline='') as f:
                        f.write(csv_text)
                    wrote_any = True
            except Exception:
                pass
        if pbp:
            try:
                csv_text = generate_pbp_csv(game, pbp, summary, teams_meta)
                if csv_text.strip():
                    with open(pbp_path, 'w', encoding='utf-8', newline='') as f:
                        f.write(csv_text)
                    wrote_any = True
            except Exception:
                pass
        print(" done" if wrote_any else " no data")
        time.sleep(0.02)


if __name__ == '__main__':
    main()
