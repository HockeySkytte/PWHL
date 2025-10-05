import os
import csv
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

DATA_SHOTS_DIR = os.path.join(os.path.dirname(__file__), 'Data', 'Play-by-Play')

class ReportDataStore:
    """In-memory aggregation for Report page metrics.

    Loads per-game *_shots.csv files (which include all PBP events we need) and derives:
      - Per attempt classification (Corsi/Fenwick/Shot/Goal)
      - Team for/against tallies by game
      - Strength filtering (exact string match for now)
    """
    def __init__(self):
        self.loaded = False
        self.rows: List[Dict[str, Any]] = []   # Raw normalized attempts/events subset
        self.game_team_stats: Dict[Tuple[str,str], Dict[str, float]] = {}  # (game_id, team) -> metrics
        self.game_meta: Dict[str, Dict[str, Any]] = {}  # game_id -> {date, season, state}
        # TOI cache from lineup CSVs: (game_id, player_name) -> seconds
        self.toi_lookup: Dict[Tuple[str,str], int] = {}
        self._lineups_loaded: set[str] = set()

    def _load_lineups_for_game(self, game_id: str):
        """Lazy-load specific lineup CSV for a game id; avoid re-scanning directory each call."""
        if not game_id or game_id in self._lineups_loaded:
            return
        lineups_dir = os.path.join(os.path.dirname(__file__), 'Data', 'Lineups')
        if not os.path.isdir(lineups_dir):
            self._lineups_loaded.add(game_id)
            return
        # Likely filenames
        candidates = [f"{game_id}_teams.csv", f"{game_id}_lineups.csv", f"{game_id}.csv"]
        matched_files = [c for c in candidates if os.path.isfile(os.path.join(lineups_dir, c))]
        # Fallback: scan once if none of the candidates exist
        if not matched_files:
            for fname in os.listdir(lineups_dir):
                if fname.endswith('.csv') and str(game_id) in fname:
                    matched_files.append(fname)
        for fname in matched_files:
            fpath = os.path.join(lineups_dir, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        gid = str(row.get('Game ID') or row.get('game_id') or row.get('gameId') or '')
                        if gid != str(game_id):
                            continue
                        name = (row.get('Name') or row.get('player') or '').strip()
                        if not name:
                            continue
                        try:
                            toi = int(row.get('TOI') or 0)
                        except Exception:
                            toi = 0
                        self.toi_lookup[(gid, name)] = toi
            except Exception:
                continue
        self._lineups_loaded.add(game_id)

    def load(self, force: bool = False):
        if self.loaded and not force:
            return
        self.rows.clear()
        self.game_team_stats.clear()
        self.game_meta.clear()
        if not os.path.isdir(DATA_SHOTS_DIR):
            self.loaded = True
            return
        for fname in os.listdir(DATA_SHOTS_DIR):
            if not fname.endswith('_shots.csv'):
                continue
            fpath = os.path.join(DATA_SHOTS_DIR, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Store basic meta once (augment with home/away if available in CSV export)
                        gid = str(row.get('game_id') or '')
                        if gid and gid not in self.game_meta:
                            self.game_meta[gid] = {
                                'date': row.get('game_date') or '',
                                'season': row.get('season') or '',
                                'state': row.get('state') or '',
                                'home_team': row.get('team_home') or '',
                                'away_team': row.get('team_away') or ''
                            }
                        # Only process shot-attempt related events (Shot, Goal, Block, Miss) + Goals
                        ev = (row.get('event') or '').strip()
                        # Include penalty for plotting (if coordinates exist later) & filtering, even if no coords
                        if ev not in ('Shot','Goal','Block','Miss','Penalty'):
                            continue
                        strength = row.get('strength') or ''
                        team = row.get('team') or ''
                        home = row.get('team_home') or ''
                        away = row.get('team_away') or ''
                        venue = row.get('venue') or ''
                        period = row.get('period') or ''
                        shooter = row.get('p1_name') or ''
                        assist1 = row.get('p2_name') or ''
                        assist2 = row.get('p3_name') or ''
                        goalie = row.get('goalie_name') or ''
                        # On-ice player names (home / away) come as hyphen-separated in export (see export_utils)
                        def parse_onice(txt: str) -> List[str]:
                            """Parse on-ice players string.
                            Export uses ' - ' (space-hyphen-space) as delimiter between players.
                            Player names can contain hyphens (e.g., 'Marie-Philip Poulin'), so we ONLY
                            split on the exact ' - ' sequence. If that sequence is absent, treat the
                            whole string as a single player name (after stripping)."""
                            if not txt:
                                return []
                            if ' - ' in txt:
                                return [p.strip() for p in txt.split(' - ') if p.strip()]
                            return [txt.strip()]
                        home_on = parse_onice(row.get('home_players_names') or '')
                        away_on = parse_onice(row.get('away_players_names') or '')
                        on_ice_all = sorted({*home_on, *away_on})
                        # Coordinates
                        try:
                            x = float(row.get('x') or '')
                        except Exception:
                            x = None
                        try:
                            y = float(row.get('y') or '')
                        except Exception:
                            y = None
                        # Determine shooting team for Corsi logic:
                        # For Block rows we now record the SHOOTING team (export logic adjusted) so we can treat uniformly.
                        shooting_team = team
                        # Attempt flags
                        is_goal = (ev == 'Goal')
                        is_shot = ev in ('Shot','Goal')
                        is_block = (ev == 'Block')
                        is_miss = (ev == 'Miss')
                        is_corsi = ev in ('Shot','Goal','Miss','Block')
                        is_fenwick = ev in ('Shot','Goal','Miss')  # Unblocked attempts
                        # Build row
                        self.rows.append({
                            'game_id': gid,
                            'date': self.game_meta[gid]['date'] if gid else '',
                            'season': self.game_meta[gid]['season'] if gid else '',
                            'state': self.game_meta[gid]['state'] if gid else '',
                            'team_for': shooting_team,
                            'team_against': away if shooting_team == home else home if shooting_team == away else '',
                            'strength': strength,
                            'event': ev,
                            'period': period,
                            'x': x,
                            'y': y,
                            'shooter': shooter,
                            'assist1': assist1,
                            'assist2': assist2,
                            'goalie': goalie,
                            'is_goal': is_goal,
                            'is_shot': is_shot,
                            'is_block': is_block,
                            'is_miss': is_miss,
                            'is_corsi': is_corsi,
                            'is_fenwick': is_fenwick,
                            'on_ice_home': home_on,
                            'on_ice_away': away_on,
                            'on_ice_all': on_ice_all,
                        })
            except Exception:
                continue
        # Aggregate per game/team
        for r in self.rows:
            gid = r['game_id']
            team = r['team_for']
            opp = r['team_against']
            key_for = (gid, team)
            key_against = (gid, opp)
            if key_for not in self.game_team_stats:
                self.game_team_stats[key_for] = self._blank_metrics()
            if opp and key_against not in self.game_team_stats:
                self.game_team_stats[key_against] = self._blank_metrics()
            # Update for team
            m_for = self.game_team_stats[key_for]
            if r['is_corsi']:
                if team == r['team_for']:
                    m_for['CF'] += 1
                else:
                    m_for['CA'] += 1
            if r['is_fenwick']:
                m_for['FF'] += 1
            if r['is_shot']:
                m_for['SF'] += 1
            if r['is_goal']:
                m_for['GF'] += 1
            # Update against (mirrored)
            if opp:
                m_against = self.game_team_stats[key_against]
                if r['is_corsi']:
                    if opp == r['team_for']:
                        m_against['CF'] += 1
                    else:
                        m_against['CA'] += 1
                if r['is_fenwick']:
                    m_against['FA'] += 1
                if r['is_shot']:
                    m_against['SA'] += 1
                if r['is_goal']:
                    m_against['GA'] += 1
        # Orientation normalization: ensure offensive direction (for shooting team) is positive X
        # Group by (game_id, period, team_for) and compute sum of x; if negative, flip x,y.
        group_sums: Dict[Tuple[str,str,str], float] = {}
        for r in self.rows:
            if r.get('x') is None:
                continue
            key = (r['game_id'], r['period'], r['team_for'])
            group_sums[key] = group_sums.get(key, 0.0) + (r['x'] or 0.0)
        group_sign: Dict[Tuple[str,str,str], int] = {}
        for key, total in group_sums.items():
            group_sign[key] = 1 if total >= 0 else -1
        for r in self.rows:
            if r.get('x') is None or r.get('y') is None:
                r['adj_x'] = None
                r['adj_y'] = None
                continue
            sign = group_sign.get((r['game_id'], r['period'], r['team_for']), 1)
            r['adj_x'] = r['x'] * sign
            r['adj_y'] = r['y'] * sign
        self.loaded = True

    def _blank_metrics(self) -> Dict[str, float]:
        return {k:0.0 for k in ('CF','CA','FF','FA','SF','SA','GF','GA')}

    def _pct(self, numer: float, denom: float) -> Optional[float]:
        return round(numer/denom*100,1) if denom > 0 else None

    def _parse_strength(self, s: str) -> Optional[Tuple[int,int]]:
        # Expect formats like '5v4', '4v5', '5v5', '3v5'; return (for, against)
        if 'v' not in s: return None
        try:
            a,b = s.lower().split('v',1)
            return int(a), int(b)
        except Exception:
            return None

    def _classify_strength(self, row_strength: str, vantage: str, is_for_row: bool) -> str:
        """Return simplified strength class (5v5, PP, SH) from vantage team POV.
        row_strength is always expressed from team_for perspective in the raw data.
        If this row is an 'against' row relative to vantage team, invert manpower.
        """
        parsed = self._parse_strength(row_strength)
        if not parsed:
            return row_strength or ''
        for_n, against_n = parsed
        # If row is opponent (against) relative to vantage, swap perspective
        if not is_for_row:
            for_n, against_n = against_n, for_n
        # Explicit mapping according to spec
        if for_n == against_n:
            # Distinguish pure 5v5 from other even states (4v4,3v3 -> EV aggregate bucket)
            if for_n == 5:
                return '5v5'
            if for_n in (4,3):
                return 'EV'
            return 'EV'
        # Power play: our side has more skaters OR one of listed PP combos
        pp_pairs = {(5,4),(5,3),(4,3)}
        sh_pairs = {(4,5),(3,5),(3,4)}
        if (for_n, against_n) in pp_pairs:
            return 'PP'
        if (for_n, against_n) in sh_pairs:
            return 'SH'
        # Fallback to numeric comparison
        return 'PP' if for_n > against_n else 'SH'

    def compute_kpis(self, team: str = 'All', strength: str = 'All', season: str = 'All', date_from: str = '', date_to: str = '', segment: str = 'all', perspective: str='For', games: List[str]=None, players: List[str]=None, opponents: List[str]=None, periods: List[str]=None, events: List[str]=None, strengths_multi: List[str]=None, goalies: List[str]=None, seasons_multi: List[str]=None, onice: List[str]=None) -> Dict[str, Any]:
        """Compute KPI metrics.

        Added on-ice AND filter (onice list) – if provided, only retain rows where *all*
        selected players are on the ice for that event (either side). This differs from
        the existing shooter 'players' filter which is OR against shooter only.
        """
        self.load()
        games = games or []
        players = players or []
        opponents = opponents or []
        periods = periods or []
        events = events or []
        strengths_multi = strengths_multi or []
        goalies = goalies or []
        seasons_multi = seasons_multi or []
        # Support optional on-ice AND list (attribute injected by flask layer if present)
        onice = onice or []
        # When no specific team chosen we treat league aggregate (percentages become 50 by construction)
        if team == 'All':
            rows = [r for r in self.rows]
            # Season/date filters
            if seasons_multi:
                rows = [r for r in rows if r['season'] in seasons_multi]
            elif season != 'All':
                rows = [r for r in rows if r['season'] == season]
            if games:
                rows = [r for r in rows if r['game_id'] in games]
            if players:
                rows = [r for r in rows if r.get('shooter') in players]
            if opponents:
                rows = [r for r in rows if r['team_against'] in opponents]
            if periods:
                rows = [r for r in rows if r['period'] in periods]
            if events:
                rows = [r for r in rows if r['event'] in events]
            if strengths_multi:
                rows = [r for r in rows if r['strength'] in strengths_multi]
            if goalies:
                rows = [r for r in rows if r.get('goalie') in goalies]
            if onice:
                rows = [r for r in rows if all(p in (r.get('on_ice_all') or []) for p in onice)]
            if date_from:
                rows = [r for r in rows if r['date'] >= date_from]
            if date_to:
                rows = [r for r in rows if r['date'] <= date_to]
            game_ids_ordered = sorted({r['game_id'] for r in rows}, key=lambda g: self.game_meta.get(g,{}).get('date',''))
            if segment.lower() in ('last5','last_5'):
                keep = set(game_ids_ordered[-5:])
                rows = [r for r in rows if r['game_id'] in keep]
            elif segment.lower() in ('last10','last_10'):
                keep = set(game_ids_ordered[-10:])
                rows = [r for r in rows if r['game_id'] in keep]
            # Strength filter (classification from each row POV). If user selected PP/SH/5v5 we include both teams' rows that match that class.
            if strength != 'All':
                filtered = []
                for r in rows:
                    s_class = self._classify_strength(r['strength'], 'All', True)
                    if s_class == strength:
                        filtered.append(r)
                rows = filtered
            # Aggregate league-level (CF is total attempts; CA equals CF => 50%)
            CF = sum(1 for r in rows if r['is_corsi'])
            FF = sum(1 for r in rows if r['is_fenwick'])
            SF = sum(1 for r in rows if r['is_shot'])
            GF = sum(1 for r in rows if r['is_goal'])
            CA,FA,SA,GA = CF,FF,SF,GF
        else:
            # Build separate lists for rows where the selected team is the shooter (for) vs opponent shooter (against)
            rows_for = [r for r in self.rows if r['team_for'] == team]
            rows_against = [r for r in self.rows if r['team_against'] == team]
            # Apply season/date filters first
            if seasons_multi:
                rows_for = [r for r in rows_for if r['season'] in seasons_multi]
                rows_against = [r for r in rows_against if r['season'] in seasons_multi]
            elif season != 'All':
                rows_for = [r for r in rows_for if r['season'] == season]
                rows_against = [r for r in rows_against if r['season'] == season]
            if games:
                rows_for = [r for r in rows_for if r['game_id'] in games]
                rows_against = [r for r in rows_against if r['game_id'] in games]
            if players:
                rows_for = [r for r in rows_for if r.get('shooter') in players]
            if opponents:
                rows_for = [r for r in rows_for if r['team_against'] in opponents]
                rows_against = [r for r in rows_against if r['team_for'] in opponents]
            if periods:
                rows_for = [r for r in rows_for if r['period'] in periods]
                rows_against = [r for r in rows_against if r['period'] in periods]
            if events:
                rows_for = [r for r in rows_for if r['event'] in events]
                rows_against = [r for r in rows_against if r['event'] in events]
            if strengths_multi:
                rows_for = [r for r in rows_for if r['strength'] in strengths_multi]
                rows_against = [r for r in rows_against if r['strength'] in strengths_multi]
            if goalies:
                rows_against = [r for r in rows_against if r.get('goalie') in goalies]  # goalie belongs to against rows perspective
                rows_for = [r for r in rows_for if r.get('goalie') in goalies]
            if onice:
                rows_for = [r for r in rows_for if all(p in (r.get('on_ice_all') or []) for p in onice)]
                rows_against = [r for r in rows_against if all(p in (r.get('on_ice_all') or []) for p in onice)]
            if date_from:
                rows_for = [r for r in rows_for if r['date'] >= date_from]
                rows_against = [r for r in rows_against if r['date'] >= date_from]
            if date_to:
                rows_for = [r for r in rows_for if r['date'] <= date_to]
                rows_against = [r for r in rows_against if r['date'] <= date_to]
            # Segment filtering uses union of game ids ordered by date from vantage perspective (games where team appears)
            game_ids_ordered = sorted({*(r['game_id'] for r in rows_for), *(r['game_id'] for r in rows_against)}, key=lambda g: self.game_meta.get(g,{}).get('date',''))
            if segment.lower() in ('last5','last_5'):
                keep = set(game_ids_ordered[-5:])
                rows_for = [r for r in rows_for if r['game_id'] in keep]
                rows_against = [r for r in rows_against if r['game_id'] in keep]
            elif segment.lower() in ('last10','last_10'):
                keep = set(game_ids_ordered[-10:])
                rows_for = [r for r in rows_for if r['game_id'] in keep]
                rows_against = [r for r in rows_against if r['game_id'] in keep]
            # Strength filtering. Strength selector may be full form (5v5) OR aggregated PP/SH.
            if strength != 'All':
                def strength_match(r, is_for):
                    if strength in ('PP','SH'):
                        return self._classify_strength(r['strength'], team, is_for) == strength
                    # literal match (e.g., '5v5', '4v5', '5v4') within vantage perspective: if filter is '5v5', accept only 5v5.
                    if strength == '5v5':
                        return self._classify_strength(r['strength'], team, is_for) == '5v5'
                    # direct raw strength fallback
                    return r['strength'] == strength
                rows_for = [r for r in rows_for if strength_match(r, True)]
                rows_against = [r for r in rows_against if strength_match(r, False)]
            # Aggregate
            CF = sum(1 for r in rows_for if r['is_corsi'])
            CA = sum(1 for r in rows_against if r['is_corsi'])
            FF = sum(1 for r in rows_for if r['is_fenwick'])
            FA = sum(1 for r in rows_against if r['is_fenwick'])
            SF = sum(1 for r in rows_for if r['is_shot'])
            SA = sum(1 for r in rows_against if r['is_shot'])
            GF = sum(1 for r in rows_for if r['is_goal'])
            GA = sum(1 for r in rows_against if r['is_goal'])
            rows = rows_for  # for sample size representation we display FOR rows count (attempt rows) - can adjust later
        # Percentages / derived
        cfpct = self._pct(CF, CF+CA)
        ffpct = self._pct(FF, FF+FA)
        sfpct = self._pct(SF, SF+SA)
        gfpct = self._pct(GF, GF+GA)
        shpct = round(GF/SF*100,1) if SF>0 else None
        svpct = round((1 - GA/SA)*100,1) if SA>0 else None
        pdo = round((shpct or 0)+(svpct or 0),1) if shpct is not None and svpct is not None else None
        return {
            'filters': {
                'team': team,
                'strength': strength,
                'season': season,
                'date_from': date_from,
                'date_to': date_to,
                'segment': segment,
                'perspective': perspective,
            },
            'sample': {
                'games': len({r['game_id'] for r in rows}),
                'attempt_rows': len(rows)
            },
            'metrics': {
                'CF': CF, 'CA': CA, 'CF%': cfpct,
                'FF': FF, 'FA': FA, 'FF%': ffpct,
                'SF': SF, 'SA': SA, 'SF%': sfpct,
                'xGF': None, 'xGA': None, 'xGF%': None,
                'GF': GF, 'GA': GA, 'GF%': gfpct,
                'Sh%': shpct, 'Sv%': svpct, 'PDO': pdo
            }
        }

    def shotmap(self, team: str='All', strength: str='All', season: str='All', date_from: str='', date_to: str='', segment: str='all', perspective: str='For', games: List[str]=None, players: List[str]=None, opponents: List[str]=None, periods: List[str]=None, events: List[str]=None, strengths_multi: List[str]=None, goalies: List[str]=None, seasons_multi: List[str]=None, onice: List[str]=None) -> Dict[str, Any]:
        self.load()
        games = games or []
        players = players or []
        opponents = opponents or []
        periods = periods or []
        events = events or []
        strengths_multi = strengths_multi or []
        goalies = goalies or []
        seasons_multi = seasons_multi or []
        onice = onice or []
        rows = self.rows
        if team != 'All':
            if perspective.lower() == 'against':
                rows = [r for r in rows if r['team_against'] == team]
            elif perspective.lower() == 'all':
                rows = [r for r in rows if r['team_for'] == team or r['team_against'] == team]
            else:  # For
                rows = [r for r in rows if r['team_for'] == team]
        if strength != 'All':
            rows = [r for r in rows if r['strength'] == strength]
        if seasons_multi:
            rows = [r for r in rows if r['season'] in seasons_multi]
        elif season != 'All':
            rows = [r for r in rows if r['season'] == season]
        if games:
            rows = [r for r in rows if r['game_id'] in games]
        if players:
            rows = [r for r in rows if r.get('shooter') in players]
        if opponents:
            rows = [r for r in rows if r['team_against'] in opponents or r['team_for'] in opponents]
        if periods:
            rows = [r for r in rows if r['period'] in periods]
        if events:
            rows = [r for r in rows if r['event'] in events]
        if strengths_multi:
            rows = [r for r in rows if r['strength'] in strengths_multi]
        if goalies:
            rows = [r for r in rows if r.get('goalie') in goalies]
        if onice:
            rows = [r for r in rows if all(p in (r.get('on_ice_all') or []) for p in onice)]
        if date_from:
            rows = [r for r in rows if r['date'] >= date_from]
        if date_to:
            rows = [r for r in rows if r['date'] <= date_to]
        game_ids_ordered = sorted({r['game_id'] for r in rows}, key=lambda g: self.game_meta.get(g,{}).get('date',''))
        if segment.lower() in ('last5','last_5'):
            keep = set(game_ids_ordered[-5:])
            rows = [r for r in rows if r['game_id'] in keep]
        elif segment.lower() in ('last10','last_10'):
            keep = set(game_ids_ordered[-10:])
            rows = [r for r in rows if r['game_id'] in keep]
        # Build attempt list for plotting (Shots, Goals, Misses, Blocks) with coordinates
        attempts = []
        for r in rows:
            # Include Penalty for tooltip visualization if coordinates later desired (currently penalties lack coords so skipped by None check)
            if r['event'] not in ('Shot','Goal','Miss','Block','Penalty'):
                continue
            if r.get('x') is None or r.get('y') is None:
                continue
            attempts.append({
                'x': r['x'],  # raw
                'y': r['y'],
                'adj_x': r.get('adj_x'),  # oriented so offense for shooter is +X
                'adj_y': r.get('adj_y'),
                'event': r['event'],
                'goal': 1 if r['is_goal'] else 0,
                'strength': r['strength'],
                'period': r['period'],
                'forTeam': r['team_for'],
                'againstTeam': r['team_against'],
                'shooter': r.get('shooter'),
                'goalie': r.get('goalie'),
                'season': r.get('season'),
                'state': r.get('state'),
            })
        return {'count': len(attempts), 'games': len(game_ids_ordered), 'attempts': attempts}

    # ---------------- Table Aggregations -----------------
    def _apply_common_filters(self, rows, **kwargs):
        team = kwargs.get('team','All')
        season = kwargs.get('season','All')
        season_state = kwargs.get('season_state','All')
        date_from = kwargs.get('date_from','')
        date_to = kwargs.get('date_to','')
        games = kwargs.get('games') or []
        players = kwargs.get('players') or []
        opponents = kwargs.get('opponents') or []
        periods = kwargs.get('periods') or []
        events = kwargs.get('events') or []
        strengths_multi = kwargs.get('strengths_multi') or []
        goalies = kwargs.get('goalies') or []
        seasons_multi = kwargs.get('seasons_multi') or []
        onice = kwargs.get('onice') or []
        strength = kwargs.get('strength','All')
        segment = kwargs.get('segment','all')
        row_strength_independent = kwargs.get('row_strength_independent', False)
        # base team scoping left to caller; here apply rest
        if seasons_multi:
            rows=[r for r in rows if r['season'] in seasons_multi]
        elif season!='All':
            rows=[r for r in rows if r['season']==season]
        if season_state != 'All':
            rows=[r for r in rows if r.get('state')==season_state]
        if games:
            rows=[r for r in rows if r['game_id'] in games]
        if players:
            rows=[r for r in rows if r.get('shooter') in players]
        if opponents:
            rows=[r for r in rows if r['team_against'] in opponents or r['team_for'] in opponents]
        if periods:
            rows=[r for r in rows if r['period'] in periods]
        if events:
            rows=[r for r in rows if r['event'] in events]
        if strengths_multi:
            rows=[r for r in rows if r['strength'] in strengths_multi]
        if goalies:
            rows=[r for r in rows if r.get('goalie') in goalies]
        if onice:
            rows=[r for r in rows if all(p in (r.get('on_ice_all') or []) for p in onice)]
        if date_from:
            rows=[r for r in rows if r['date']>=date_from]
        if date_to:
            rows=[r for r in rows if r['date']<=date_to]
        # segment cutting
        game_ids_ordered=sorted({r['game_id'] for r in rows}, key=lambda g:self.game_meta.get(g,{}).get('date',''))
        if segment.lower() in ('last5','last_5'):
            keep=set(game_ids_ordered[-5:]); rows=[r for r in rows if r['game_id'] in keep]
        elif segment.lower() in ('last10','last_10'):
            keep=set(game_ids_ordered[-10:]); rows=[r for r in rows if r['game_id'] in keep]
        if strength!='All':
            if row_strength_independent:
                tmp=[]
                for r in rows:
                    # classify from row's own team_for perspective
                    s_class=self._classify_strength(r['strength'], r['team_for'], True)
                    if strength in ('PP','SH'):
                        # Include both PP and SH rows so PP GA counts SH goals and vice versa
                        if s_class in ('PP','SH'): tmp.append(r)
                    elif strength=='EV':
                        if s_class in ('5v5','EV'): tmp.append(r)
                    else:
                        if s_class==strength: tmp.append(r)
                rows=tmp
            else:
                # legacy vantage-based behavior (KPIs / heatmap contexts)
                if team!='All':
                    tmp=[]
                    for r in rows:
                        s_class=self._classify_strength(r['strength'], team, r['team_for']==team)
                        if strength=='EV':
                            if s_class in ('5v5','EV'): tmp.append(r)
                        elif strength in ('PP','SH','5v5'):
                            if s_class==strength: tmp.append(r)
                        else:
                            if r['strength']==strength: tmp.append(r)
                    rows=tmp
                else:
                    tmp=[]
                    for r in rows:
                        s_class=self._classify_strength(r['strength'], 'All', True)
                        if strength=='EV':
                            if s_class in ('5v5','EV'): tmp.append(r)
                        elif strength in ('PP','SH','5v5'):
                            if s_class==strength: tmp.append(r)
                        else:
                            if r['strength']==strength: tmp.append(r)
                    rows=tmp
        return rows

    def tables_skaters_individual(self, **kwargs):
        self.load()
        rows=self.rows
        rows=self._apply_common_filters(rows, row_strength_independent=True, **kwargs)
        season_filter = kwargs.get('season','All')
        season_state_filter = kwargs.get('season_state','All')
        strength_filter = kwargs.get('strength','All')
        stats={}
        goalie_names = {r['goalie'] for r in rows if r.get('goalie')}
        def ensure_player(name: str, team: str):
            return stats.setdefault(name, {
                'player': name,
                'team': team,
                'GP': set(),
                'G':0,'A':0,'P':0,
                'PEN_taken':0,'PEN_drawn':0,
                'Shots':0,'Misses':0,'Shots_in_block':0,'Blocks':0,
                'GF':0,'GA':0
            })
        # First pass
        for r in rows:
            shooter=r.get('shooter')
            if not shooter or shooter in goalie_names:
                continue
            team=r['team_for']
            s=ensure_player(shooter, team)
            s['GP'].add(r['game_id'])
            self._load_lineups_for_game(r['game_id'])
            if r['is_goal']:
                s['G']+=1
                if r.get('assist1') and r['assist1'] not in goalie_names:
                    ensure_player(r['assist1'], team)['GP'].add(r['game_id'])
                if r.get('assist2') and r['assist2'] not in goalie_names:
                    ensure_player(r['assist2'], team)['GP'].add(r['game_id'])
            if r['is_shot']: s['Shots']+=1
            if r['is_miss']: s['Misses']+=1
            if r['is_block']: s['Shots_in_block']+=1
        # Second pass assists & penalties
        for r in rows:
            if r['event']=='Goal':
                for a_field in ('assist1','assist2'):
                    a=r.get(a_field)
                    if a and a not in goalie_names:
                        s=ensure_player(a, r['team_for'])
                        s['A']+=1
            if r['event']=='Penalty':
                shooter=r.get('shooter')
                if shooter and shooter not in goalie_names:
                    ensure_player(shooter, r['team_for'])['PEN_taken']+=1
        # On-ice goals
        for r in rows:
            if r['event']!='Goal': continue
            gid=r['game_id']
            meta=self.game_meta.get(gid, {})
            home=meta.get('home_team')
            shooting_team=r['team_for']
            shooter=r.get('shooter')
            if shooting_team==home:
                for_players=r.get('on_ice_home') or []
                against_players=r.get('on_ice_away') or []
            else:
                for_players=r.get('on_ice_away') or []
                against_players=r.get('on_ice_home') or []
            if shooter and shooter not in for_players and shooter not in goalie_names:
                for_players=list(for_players)+[shooter]
            for p in for_players:
                if p in goalie_names: continue
                s=ensure_player(p, shooting_team); s['GF']+=1; s['GP'].add(gid)
            for p in against_players:
                if p in goalie_names: continue
                s=ensure_player(p, r['team_against']); s['GA']+=1; s['GP'].add(gid)
        out=[]
        for s in stats.values():
            s['GP']=len(s['GP'])
            s['P']=s['G']+s['A']
            s['Sh%']=round(s['G']/s['Shots']*100,1) if s['Shots']>0 else 0
            total_toi_secs=sum(secs for (gid,name),secs in self.toi_lookup.items() if name==s['player'])
            s['TOI']=round(total_toi_secs/60,1) if total_toi_secs else 0.0
            total_goals=s['GF']+s['GA']
            s['GF%']=round(s['GF']/total_goals*100,1) if total_goals>0 else None
            s['Season']=season_filter
            s['Season_State']=season_state_filter
            s['Strength']=strength_filter
            out.append(s)
        out.sort(key=lambda x:(-x['P'],-x['G'],x['player']))
        return out

    def tables_skaters_onice(self, **kwargs):
        return []

    def tables_goalies(self, **kwargs):
        return []

    def tables_teams(self, **kwargs):
        return []


report_store = ReportDataStore()