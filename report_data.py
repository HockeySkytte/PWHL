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
        # Store video-capable events separately (populated below)
        self.video_events: List[Dict[str, Any]] = []
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
                        # Only process shot-attempt related events (Shot, Goal, Block, Miss) + Penalties (for filtering)
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
                        rec = {
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
                        }
                        # Always add video_url and video_time keys, even if missing in CSV
                        rec['video_url'] = row.get('video_url') or row.get('Video URL') or ''
                        rec['video_time'] = row.get('video_time') or row.get('Video Time') or ''
                        self.rows.append(rec)
                        # Capture video-tagged events (any non-empty URL). If time missing or invalid, default to 0.
                        if rec['video_url'] and rec['event'] in ('Shot','Goal','Block','Miss','Penalty'):
                            raw_vtime = rec.get('video_time')
                            vtime: int = 0
                            if raw_vtime not in (None, '', 'NaN'):
                                try:
                                    vtime = int(float(raw_vtime))
                                except Exception:
                                    vtime = 0
                            self.video_events.append({
                                'game_id': rec['game_id'],
                                'season': rec['season'],
                                'state': rec['state'],
                                'team': rec['team_for'],
                                'opponent': rec.get('team_against',''),
                                'event': rec['event'],
                                'player': rec['shooter'] or '',
                                'video_url': rec['video_url'],
                                'video_time': vtime,
                                'period': rec.get('period',''),
                                'strength': rec.get('strength',''),
                                'date': rec.get('date',''),
                                'has_explicit_time': bool(raw_vtime not in (None, '', 'NaN'))
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

    def compute_kpis(self, team: str = 'All', strength: str = 'All', season: str = 'All', date_from: str = '', date_to: str = '', segment: str = 'all', perspective: str='For', games=None, players=None, opponents=None, periods=None, events=None, strengths_multi=None, goalies=None, seasons_multi=None, onice=None) -> Dict[str, Any]:
        """Compute KPI metrics.

        Added on-ice AND filter (onice list) – if provided, only retain rows where *all*
        selected players are on the ice for that event (either side). This differs from
        the existing shooter 'players' filter which is OR against shooter only.
        
        VERSION: Fixed string-to-list conversion for filter parameters (2025-11-23)
        """
        self.load()
        # Convert comma-separated strings to lists (from Flask API)
        games = games.split(',') if isinstance(games, str) and games else (games or [])
        players = players.split(',') if isinstance(players, str) and players else (players or [])
        opponents = opponents.split(',') if isinstance(opponents, str) and opponents else (opponents or [])
        periods = periods.split(',') if isinstance(periods, str) and periods else (periods or [])
        events = events.split(',') if isinstance(events, str) and events else (events or [])
        strengths_multi = strengths_multi.split(',') if isinstance(strengths_multi, str) and strengths_multi else (strengths_multi or [])
        goalies = goalies.split(',') if isinstance(goalies, str) and goalies else (goalies or [])
        seasons_multi = seasons_multi.split(',') if isinstance(seasons_multi, str) and seasons_multi else (seasons_multi or [])
        # Support optional on-ice AND list (attribute injected by flask layer if present)
        onice = onice.split(',') if isinstance(onice, str) and onice else (onice or [])
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
                # Do NOT filter rows_for by goalie - those are shots BY the team, not shots the goalie faced
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
        # xG aggregations
        def _xg_sum(rows_list):
            total = 0.0
            for rr in rows_list:
                xv = rr.get('xG')
                if xv is None or xv == '':
                    continue
                try:
                    total += float(xv)
                except Exception:
                    continue
            return round(total, 2)
        if team == 'All':
            xGF = _xg_sum(rows)
            xGA = xGF
        else:
            xGF = _xg_sum(rows_for)
            xGA = _xg_sum(rows_against)
        xgfpct = self._pct(xGF, xGF + xGA)
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
                'xGF': xGF, 'xGA': xGA, 'xGF%': xgfpct,
                'GF': GF, 'GA': GA, 'GF%': gfpct,
                'Sh%': shpct, 'Sv%': svpct, 'PDO': pdo
            }
        }

    def shotmap(self, team: str='All', strength: str='All', season: str='All', date_from: str='', date_to: str='', segment: str='all', perspective: str='For', games=None, players=None, opponents=None, periods=None, events=None, strengths_multi=None, goalies=None, seasons_multi=None, onice=None) -> Dict[str, Any]:
        self.load()
        # Convert comma-separated strings to lists (from Flask API)
        games = games.split(',') if isinstance(games, str) and games else (games or [])
        players = players.split(',') if isinstance(players, str) and players else (players or [])
        opponents = opponents.split(',') if isinstance(opponents, str) and opponents else (opponents or [])
        periods = periods.split(',') if isinstance(periods, str) and periods else (periods or [])
        events = events.split(',') if isinstance(events, str) and events else (events or [])
        strengths_multi = strengths_multi.split(',') if isinstance(strengths_multi, str) and strengths_multi else (strengths_multi or [])
        goalies = goalies.split(',') if isinstance(goalies, str) and goalies else (goalies or [])
        seasons_multi = seasons_multi.split(',') if isinstance(seasons_multi, str) and seasons_multi else (seasons_multi or [])
        onice = onice.split(',') if isinstance(onice, str) and onice else (onice or [])
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

    # ---------------- Video Events -----------------
    def video_events_list(self, team: str='All', season: str='All', season_state: str='All', games=None, periods=None, events=None, strengths=None, players=None, opponents=None, date_from: str='', date_to: str='') -> List[Dict[str, Any]]:
        """Return flat list of events that have usable video clips.

        Currently very light filtering: optional team (shooter team), season, season_state, games list.
        """
        self.load()
        games = games or []
        periods = periods or []
        events = events or []
        strengths = strengths or []
        players = players or []
        opponents = opponents or []
        rows = self.video_events
        if team != 'All':
            # Participation semantics: include events where selected team is shooter OR opponent
            rows = [r for r in rows if r['team'] == team or r.get('opponent') == team]
        if season != 'All':
            rows = [r for r in rows if r['season'] == season]
        if season_state != 'All':
            rows = [r for r in rows if r['state'] == season_state]
        if games:
            rows = [r for r in rows if r['game_id'] in games]
        if periods:
            periods_set = {str(p) for p in periods}
            rows = [r for r in rows if str(r.get('period')) in periods_set]
        if events:
            events_set = set(events)
            rows = [r for r in rows if r.get('event') in events_set]
        if strengths:
            strengths_set = set(strengths)
            rows = [r for r in rows if r.get('strength') in strengths_set]
        if players:
            players_set = set(players)
            rows = [r for r in rows if r.get('player') in players_set]
        if opponents:
            opp_set = set(opponents)
            rows = [r for r in rows if r.get('opponent') in opp_set]
        if date_from:
            rows = [r for r in rows if r.get('date','') >= date_from]
        if date_to:
            rows = [r for r in rows if r.get('date','') <= date_to]
        # Sort by game then video_time
        rows = sorted(rows, key=lambda r: (r['game_id'], r['video_time']))
        # Normalize response shape for frontend
        return [
            {
                'game_id': r['game_id'],
                'team': r['team'],
                'event': r['event'],
                'player': r['player'],
                'video_url': r['video_url'],
                'video_time': r['video_time'],
                'period': r.get('period'),
                'strength': r.get('strength')
            }
            for r in rows
        ]

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
        """Minimal skaters table: correct individual G, A, P with basic shooting stats.

        Intentionally ignores on-ice, strength splits, GF/GA, and complex filters
        to eliminate sources of inflation. Only deduplicates goal events so a
        duplicated goal row cannot inflate G/A.
        
        If by_game=True, returns one row per player per game with game details.
        """
        self.load()
        # Keep simple optional seasonal labels for UI consistency and now apply those filters
        season_filter = kwargs.get('season','All')
        season_state_filter = kwargs.get('season_state','All')
        strength_filter = kwargs.get('strength','All')
        by_game = kwargs.get('by_game', False)

        rows = self.rows
        if season_filter != 'All':
            rows = [r for r in rows if r.get('season') == season_filter]
        if season_state_filter != 'All':
            rows = [r for r in rows if r.get('state') == season_state_filter]
        if strength_filter != 'All':
            tmp=[]
            for r in rows:
                s_class = self._classify_strength(r.get('strength',''), r.get('team_for',''), True)
                if strength_filter == 'EV':
                    if s_class in ('5v5','EV'): tmp.append(r)
                elif strength_filter in ('PP','SH','5v5'):
                    if s_class == strength_filter: tmp.append(r)
                else:
                    if r.get('strength') == strength_filter: tmp.append(r)
            rows = tmp
        
        # Initialize stats dictionaries based on mode
        stats_by_game: Dict[Tuple[str, str], Dict[str, Any]] = {}
        stats_agg: Dict[str, Any] = {}
        
        goalie_names = {r['goalie'] for r in rows if r.get('goalie')}

        def ensure_player(name: str, team: str, game_id = None):
            if by_game:
                key = (name, game_id)
                if key not in stats_by_game:
                    stats_by_game[key] = {
                        'player': name,
                        'team': team,
                        'game_id': game_id,
                        'opponent': '',
                        'date': '',
                        'venue': '',
                        'G':0,'A':0,'P':0,
                        'Shots':0,'Misses':0,'Shots_in_block':0,
                        'PEN_taken':0,'PEN_drawn':0,
                        'ixG': 0.0,
                    }
                return stats_by_game[key]
            else:
                return stats_agg.setdefault(name, {
                    'player': name,
                    'team': team,
                    'GP': set(),
                    'G':0,'A':0,'P':0,
                    'Shots':0,'Misses':0,'Shots_in_block':0,
                    'PEN_taken':0,'PEN_drawn':0,
                    'ixG': 0.0,
                })

        goal_keys=set()
        
        # Pre-populate all players from lineup CSVs to ensure players with no shot events are included
        # For aggregated mode, we need to track which games each player participated in based on TOI
        if by_game or not by_game:
            # Get all unique game IDs from filtered rows
            game_ids = {r['game_id'] for r in rows}
            for gid in game_ids:
                self._load_lineups_for_game(gid)
        
        # For aggregated mode, pre-populate GP from TOI data
        if not by_game:
            # Track games where each player has TOI
            for (gid, player_name), toi_secs in self.toi_lookup.items():
                if toi_secs > 0 and player_name not in goalie_names:
                    # Filter by Season and State
                    meta = self.game_meta.get(gid, {})
                    game_season = meta.get('season', '')
                    game_state = meta.get('state', '')
                    if season_filter != 'All' and game_season != season_filter:
                        continue
                    if season_state_filter != 'All' and game_state != season_state_filter:
                        continue
                    
                    # Need to determine the player's team from lineup CSV
                    lineups_dir = os.path.join(os.path.dirname(DATA_SHOTS_DIR), 'Lineups')
                    lineup_file = os.path.join(lineups_dir, f"{gid}_teams.csv")
                    if os.path.exists(lineup_file):
                        try:
                            with open(lineup_file, 'r', encoding='utf-8') as f:
                                reader = csv.DictReader(f)
                                for row in reader:
                                    if row.get('Name', '').strip() == player_name:
                                        player_team = row.get('Team', '')
                                        # Ensure player exists in stats_agg
                                        p = ensure_player(player_name, player_team, None)
                                        # Add this game to their GP
                                        p['GP'].add(gid)
                                        break
                        except:
                            pass
        
        # For by_game mode, pre-populate all players from lineup CSVs to ensure players with no shot events are included
        if by_game:
            # Get all unique game IDs from filtered rows
            game_ids = {r['game_id'] for r in rows}
            for gid in game_ids:
                self._load_lineups_for_game(gid)
                # Get game metadata
                meta = self.game_meta.get(gid, {})
                home_team = meta.get('home_team', '')
                away_team = meta.get('away_team', '')
                date = meta.get('date', '')
                season = meta.get('season', '')
                state = meta.get('state', '')
                
                # Pre-populate all players from this game's lineup
                for (lineup_gid, player_name), toi_secs in self.toi_lookup.items():
                    if lineup_gid != gid:
                        continue
                    if player_name in goalie_names:
                        continue
                    
                    # Determine team and venue from lineup data
                    # We need to scan the lineup CSV again to get team/venue info
                    lineups_dir = os.path.join(os.path.dirname(DATA_SHOTS_DIR), 'Lineups')
                    lineup_file = os.path.join(lineups_dir, f"{gid}_teams.csv")
                    if os.path.exists(lineup_file):
                        try:
                            with open(lineup_file, 'r', encoding='utf-8') as f:
                                reader = csv.DictReader(f)
                                for row in reader:
                                    if row.get('Name', '').strip() == player_name:
                                        player_team = row.get('Team', '')
                                        venue_str = row.get('Venue', '')
                                        
                                        # Create player entry if not exists
                                        p = ensure_player(player_name, player_team, gid)
                                        if not p.get('date'):
                                            p['date'] = date
                                            p['Season'] = season
                                            p['Season_State'] = state
                                            p['Strength'] = strength_filter
                                            p['venue'] = venue_str
                                            
                                            # Determine opponent
                                            if player_team == home_team:
                                                p['opponent'] = away_team
                                            elif player_team == away_team:
                                                p['opponent'] = home_team
                                        break
                        except Exception:
                            pass
        
        for r in rows:
            shooter = r.get('shooter')
            team = r.get('team_for')
            if not shooter or shooter in goalie_names or not team:
                continue
            gid = r['game_id']
            # Load TOI lineup data lazily
            self._load_lineups_for_game(gid)
            
            # For by_game mode, populate game metadata
            if by_game:
                s = ensure_player(shooter, team, gid)
                if not s.get('date'):
                    meta = self.game_meta.get(gid, {})
                    s['date'] = meta.get('date', '')
                    s['Season'] = meta.get('season', '') or r.get('season', '')
                    s['Season_State'] = meta.get('state', '') or r.get('state', '')
                    s['Strength'] = strength_filter
                    # Determine opponent and venue
                    home_team = meta.get('home_team', '')
                    away_team = meta.get('away_team', '')
                    if team == home_team:
                        s['opponent'] = away_team
                        s['venue'] = 'Home'
                    elif team == away_team:
                        s['opponent'] = home_team
                        s['venue'] = 'Away'
                    else:
                        s['opponent'] = ''
                        s['venue'] = ''
            
            # Shots / attempts
            if r['is_shot']:
                s = ensure_player(shooter, team, gid if by_game else None)
                if not by_game:
                    s['GP'].add(gid)
                s['Shots'] += 1
                xv = r.get('xG')
                if xv not in (None, ''):
                    try:
                        s['ixG'] += float(xv)
                    except Exception:
                        pass
            if r['is_miss']:
                s = ensure_player(shooter, team, gid if by_game else None)
                if not by_game:
                    s['GP'].add(gid)
                s['Misses'] += 1
            if r['is_block']:
                s = ensure_player(shooter, team, gid if by_game else None)
                s['Shots_in_block'] += 1
            # Goals (dedup)
            if r['is_goal']:
                gkey=(gid, r.get('period'), shooter, r.get('assist1'), r.get('assist2'), r.get('strength'), r.get('x'), r.get('y'))
                if gkey in goal_keys:
                    continue
                goal_keys.add(gkey)
                s = ensure_player(shooter, team, gid if by_game else None)
                if not by_game:
                    s['GP'].add(gid)
                s['G'] += 1
                xv = r.get('xG')
                if xv not in (None, ''):
                    try:
                        s['ixG'] += float(xv)
                    except Exception:
                        pass
                # Assists (only once per unique goal)
                for a_field in ('assist1','assist2'):
                    a = r.get(a_field)
                    if a and a not in goalie_names:
                        ap = ensure_player(a, team, gid if by_game else None)
                        if not by_game:
                            ap['GP'].add(gid)
                        ap['A'] += 1
                # On-ice 5v5 GF/GA attribution (basic plus/minus style): all skaters (non-goalies) on scoring side get GF, on opposing side GA
                strength_class = self._classify_strength(r.get('strength',''), r.get('team_for',''), True)
                if strength_class == '5v5':
                    home_on = r.get('on_ice_home') or []
                    away_on = r.get('on_ice_away') or []
                    scoring_team = r.get('team_for')
                    # Determine which list corresponds to scoring vs conceding
                    # We infer home team by presence of shooter in which list; fallback to team_for comparison to home/away in meta not stored here, so rely on on_ice lists containing shooter
                    shooter_in_home = shooter in home_on
                    if shooter_in_home:
                        gf_list, ga_list = home_on, away_on
                        gf_team, ga_team = scoring_team, r.get('team_against')
                    else:
                        gf_list, ga_list = away_on, home_on
                        gf_team, ga_team = scoring_team, r.get('team_against')
                    gf_team_str = gf_team or scoring_team or team
                    ga_team_str = ga_team or r.get('team_against') or ''
                    for pname in gf_list:
                        if pname and pname not in goalie_names and gf_team_str:
                            ps = ensure_player(pname, gf_team_str, gid if by_game else None)
                            ps['5v5_GF_onice'] = ps.get('5v5_GF_onice',0) + 1
                    for pname in ga_list:
                        if pname and pname not in goalie_names and ga_team_str:
                            ps = ensure_player(pname, ga_team_str, gid if by_game else None)
                            ps['5v5_GA_onice'] = ps.get('5v5_GA_onice',0) + 1
            # Penalties (taken only, optional)
            if r['event']=='Penalty':
                pen = r.get('shooter')
                if pen and pen not in goalie_names:
                    pstats = ensure_player(pen, team, gid if by_game else None)
                    if not by_game:
                        pstats['GP'].add(gid)
                    pstats['PEN_taken'] += 1

        # Finalize output
        out=[]
        stats_to_iterate = stats_by_game if by_game else stats_agg
        for rec in stats_to_iterate.values():
            if by_game:
                # For by_game mode, calculate TOI for this specific game
                gid = rec['game_id']
                toi_secs = self.toi_lookup.get((gid, rec['player']), 0)
                rec['TOI'] = round(toi_secs/60,1) if toi_secs else 0.0
            else:
                # Save the game set before converting GP to count
                filtered_games = rec['GP'] if isinstance(rec['GP'], set) else set()
                rec['GP'] = len(rec['GP'])
                # Filter TOI by games where player actually played (in GP set)
                total_toi_secs = sum(
                    secs for (gid, name), secs in self.toi_lookup.items()
                    if name == rec['player'] and gid in filtered_games
                )
                rec['TOI'] = round(total_toi_secs/60,1) if total_toi_secs else 0.0
            
            rec['P'] = rec['G'] + rec['A']
            rec['Sh%'] = round(rec['G']/rec['Shots']*100,1) if rec['Shots']>0 else 0
            # Round ixG for display
            rec['ixG'] = round(rec.get('ixG', 0.0), 2)
            # 5v5 on-ice derived: we compute GF_onice & GA_onice; present GF/GA as on-ice, and G+/- = GF_onice - GA_onice
            gf_on = rec.get('5v5_GF_onice', 0)
            ga_on = rec.get('5v5_GA_onice', 0)
            rec['5v5_GF'] = gf_on
            rec['5v5_GA'] = ga_on
            rec['5v5_G+/-'] = gf_on - ga_on
            # Add placeholder fields the frontend may expect (consistent schema)
            if not by_game:
                rec['Season'] = season_filter
                rec['Season_State'] = season_state_filter
                rec['Strength'] = strength_filter
            # Fill missing optional fields with defaults
            rec.setdefault('PEN_drawn', 0)
            # Skip players with 0 TOI in by_game mode
            if by_game and rec.get('TOI', 0) == 0:
                continue
            out.append(rec)

        out.sort(key=lambda x:(-x['P'], -x['G'], x['player']))
        return out

    def tables_skaters_onice(self, **kwargs):
        return []

    def tables_goalies(self, **kwargs):
        """Minimal goaltender table.

        Columns: Name (goalie), Team, Season, State, Strength, GP, TOI, SA, GA, Sv%, xGA, xSv%, dSv%, GSAx
        SA: all shots on goal (is_shot) faced by goalie (includes goals)
        GA: goals allowed (is_goal) faced by goalie
        Sv% = (SA-GA)/SA * 100
        
        If by_game=True, returns one row per goalie per game with game details.
        """
        self.load()
        season_filter = kwargs.get('season','All')
        season_state_filter = kwargs.get('season_state','All')
        strength_filter = kwargs.get('strength','All')
        by_game = kwargs.get('by_game', False)

        rows = self.rows
        if season_filter != 'All':
            rows = [r for r in rows if r.get('season') == season_filter]
        if season_state_filter != 'All':
            rows = [r for r in rows if r.get('state') == season_state_filter]
        if strength_filter != 'All':
            tmp=[]
            for r in rows:
                s_class = self._classify_strength(r.get('strength',''), r.get('team_for',''), True)
                if strength_filter == 'EV':
                    if s_class in ('5v5','EV'): tmp.append(r)
                elif strength_filter in ('PP','SH','5v5'):
                    if s_class == strength_filter: tmp.append(r)
                else:
                    if r.get('strength') == strength_filter: tmp.append(r)
            rows = tmp

        # Initialize stats dictionaries based on mode
        stats_by_game: Dict[Tuple[str, str], Dict[str, Any]] = {}
        stats_agg: Dict[str, Any] = {}

        def ensure_goalie(name: str, team: str, game_id = None):
            if by_game:
                key = (name, game_id)
                if key not in stats_by_game:
                    stats_by_game[key] = {
                        'player': name,
                        'team': team,
                        'game_id': game_id,
                        'opponent': '',
                        'date': '',
                        'venue': '',
                        'SA': 0,
                        'GA': 0,
                        'xGA': 0.0,
                    }
                return stats_by_game[key]
            else:
                return stats_agg.setdefault(name, {
                    'player': name,
                    'team': team,
                    'GP': set(),
                    'SA': 0,
                    'GA': 0,
                    'xGA': 0.0,
                })

        # For by_game mode, pre-populate all goalies from lineup CSVs
        if by_game:
            # Get all unique game IDs from filtered rows
            game_ids = {r['game_id'] for r in rows}
            for gid in game_ids:
                self._load_lineups_for_game(gid)
                # Get game metadata
                meta = self.game_meta.get(gid, {})
                home_team = meta.get('home_team', '')
                away_team = meta.get('away_team', '')
                date = meta.get('date', '')
                season = meta.get('season', '')
                state = meta.get('state', '')
                
                # Pre-populate all goalies from this game's lineup
                lineups_dir = os.path.join(os.path.dirname(DATA_SHOTS_DIR), 'Lineups')
                lineup_file = os.path.join(lineups_dir, f"{gid}_teams.csv")
                if os.path.exists(lineup_file):
                    try:
                        with open(lineup_file, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                player_name = row.get('Name', '').strip()
                                line = row.get('Line', '')
                                # Check if this is a goalie (Line = 'G')
                                if line == 'G' and player_name:
                                    goalie_team = row.get('Team', '')
                                    venue_str = row.get('Venue', '')
                                    
                                    # Create goalie entry if not exists
                                    g = ensure_goalie(player_name, goalie_team, gid)
                                    if not g.get('date'):
                                        g['date'] = date
                                        g['Season'] = season
                                        g['Season_State'] = state
                                        g['Strength'] = strength_filter
                                        g['venue'] = venue_str
                                        
                                        # Determine opponent
                                        if goalie_team == home_team:
                                            g['opponent'] = away_team
                                        elif goalie_team == away_team:
                                            g['opponent'] = home_team
                    except Exception:
                        pass

        for r in rows:
            g = r.get('goalie')
            if not g:
                continue
            team_against = r.get('team_against')  # goalie belongs to defending team
            if not team_against:
                continue
            gid = r['game_id']
            self._load_lineups_for_game(gid)
            
            rec = ensure_goalie(g, team_against, gid if by_game else None)
            
            # For by_game mode, populate game metadata
            if by_game and not rec.get('date'):
                meta = self.game_meta.get(gid, {})
                rec['date'] = meta.get('date', '')
                rec['Season'] = meta.get('season', '') or r.get('season', '')
                rec['Season_State'] = meta.get('state', '') or r.get('state', '')
                rec['Strength'] = strength_filter
                # Determine opponent and venue
                home_team = meta.get('home_team', '')
                away_team = meta.get('away_team', '')
                if team_against == home_team:
                    rec['opponent'] = away_team
                    rec['venue'] = 'Home'
                elif team_against == away_team:
                    rec['opponent'] = home_team
                    rec['venue'] = 'Away'
                else:
                    rec['opponent'] = ''
                    rec['venue'] = ''
            
            if not by_game:
                rec['GP'].add(r['game_id'])
            if r.get('is_shot'):
                rec['SA'] += 1
            if r.get('is_goal'):
                rec['GA'] += 1
            # Sum xGA from xG field on shots/goals against
            xv = r.get('xG')
            if xv not in (None, ''):
                try:
                    rec['xGA'] += float(xv)
                except Exception:
                    pass

        out=[]
        stats_to_iterate = stats_by_game if by_game else stats_agg
        for rec in stats_to_iterate.values():
            if by_game:
                # For by_game mode, calculate TOI for this specific game
                gid = rec['game_id']
                toi_secs = self.toi_lookup.get((gid, rec['player']), 0)
                rec['TOI'] = round(toi_secs/60,1) if toi_secs else 0.0
            else:
                # Save the game set before converting GP to count
                filtered_games = rec['GP'] if isinstance(rec['GP'], set) else set()
                rec['GP'] = len(rec['GP'])
                # Filter TOI by games where goalie actually played (in GP set)
                total_toi_secs = sum(
                    secs for (gid, name), secs in self.toi_lookup.items()
                    if name == rec['player'] and gid in filtered_games
                )
                rec['TOI'] = round(total_toi_secs/60,1) if total_toi_secs else 0.0
            
            sa = rec['SA']
            ga = rec['GA']
            rec['Sv%'] = round((sa-ga)/sa*100,1) if sa>0 else None
            # xSv% and differentials
            xga = rec.get('xGA', 0.0) or 0.0
            rec['xGA'] = round(xga, 2)
            rec['xSv%'] = round((1 - (xga/sa))*100,1) if sa>0 else None
            # dSv% = Sv% - xSv%
            rec['dSv%'] = (rec['Sv%'] - rec['xSv%']) if (rec['Sv%'] is not None and rec['xSv%'] is not None) else None
            # GSAx = xGA - GA
            rec['GSAx'] = round(xga - ga, 2)
            
            # Add placeholder fields the frontend may expect (consistent schema)
            if not by_game:
                rec['Season'] = season_filter
                rec['Season_State'] = season_state_filter
                rec['Strength'] = strength_filter
            # Skip goalies with 0 TOI in by_game mode
            if by_game and rec.get('TOI', 0) == 0:
                continue
            out.append(rec)
        # Sort: by Sv%, then GA ascending, then SA descending
        out.sort(key=lambda r: (-(r['Sv%'] if r['Sv%'] is not None else -1), r['GA'], -r['SA'], r['player']))
        return out

    def tables_teams(self, **kwargs):
        """Teams table aggregation.

        Columns: Team, Season, State, Strength, GP, CF, CA, CF%, FF, FA, FF%, SF, SA, SF%, GF, GA, GF%, xGF, xGA, xGF%, Sh%, Sv%, PDO

        Strength filter semantics:
          - season / season_state / strength passed in like other tables
          - When strength == 'EV' treat 5v5 + other even states (EV bucket) combined
          - When strength in ('PP','SH','5v5') use classified perspective from team POV
          - Otherwise if raw (e.g., '4v5') try to match directly
        """
        self.load()
        season_filter = kwargs.get('season','All')
        season_state_filter = kwargs.get('season_state','All')
        strength_filter = kwargs.get('strength','All')
        # We'll build per-team stats iterating once through rows and applying season/state filters up front
        rows = self.rows
        if season_filter != 'All':
            rows = [r for r in rows if r.get('season') == season_filter]
        if season_state_filter != 'All':
            rows = [r for r in rows if r.get('state') == season_state_filter]

        teams: Dict[str, Dict[str, Any]] = {}

        def ensure(team: str):
            return teams.setdefault(team, {
                'Team': team,
                'Season': season_filter,
                'Season_State': season_state_filter,
                'Strength': strength_filter,
                'games': set(),
                'CF':0,'CA':0,'FF':0,'FA':0,'SF':0,'SA':0,'GF':0,'GA':0,
                'xGF':0.0,'xGA':0.0
            })

        for r in rows:
            team_for = r['team_for']
            team_against = r['team_against']
            gid = r['game_id']
            # Strength filtering per vantage team; we may need to know if row qualifies for team_for and team_against separately
            def row_ok(v_team: str, is_for: bool) -> bool:
                if strength_filter == 'All':
                    return True
                s_class = self._classify_strength(r.get('strength',''), v_team, is_for)
                if strength_filter == 'EV':
                    return s_class in ('5v5','EV')
                if strength_filter in ('PP','SH','5v5'):
                    return s_class == strength_filter
                # Fallback raw match
                return r.get('strength') == strength_filter
            # Team for side accumulation
            if team_for:
                if row_ok(team_for, True):
                    recf = ensure(team_for)
                    recf['games'].add(gid)
                    if r['is_corsi']:
                        recf['CF'] += 1
                    if r['is_fenwick']:
                        recf['FF'] += 1
                    if r['is_shot']:
                        recf['SF'] += 1
                    if r['is_goal']:
                        recf['GF'] += 1
                    xv = r.get('xG')
                    if xv not in (None, ''):
                        try:
                            recf['xGF'] += float(xv)
                        except Exception:
                            pass
            # Team against side accumulation (mirrored stats as Against for that team)
            if team_against:
                if row_ok(team_against, False):
                    reca = ensure(team_against)
                    reca['games'].add(gid)
                    if r['is_corsi']:
                        reca['CA'] += 1
                    if r['is_fenwick']:
                        reca['FA'] += 1
                    if r['is_shot']:
                        reca['SA'] += 1
                    if r['is_goal']:
                        reca['GA'] += 1
                    xv = r.get('xG')
                    if xv not in (None, ''):
                        try:
                            reca['xGA'] += float(xv)
                        except Exception:
                            pass

        out=[]
        for rec in teams.values():
            CF,CA,FF,FA,SF,SA,GF,GA = rec['CF'],rec['CA'],rec['FF'],rec['FA'],rec['SF'],rec['SA'],rec['GF'],rec['GA']
            cf_pct = self._pct(CF, CF+CA)
            ff_pct = self._pct(FF, FF+FA)
            sf_pct = self._pct(SF, SF+SA)
            gf_pct = self._pct(GF, GF+GA)
            sh_pct = round(GF/SF*100,1) if SF>0 else None
            sv_pct = round((1 - GA/SA)*100,1) if SA>0 else None
            pdo = round((sh_pct or 0)+(sv_pct or 0),1) if sh_pct is not None and sv_pct is not None else None
            xGF = round(rec.get('xGF', 0.0), 2)
            xGA = round(rec.get('xGA', 0.0), 2)
            xgf_pct = self._pct(xGF, xGF + xGA)
            out.append({
                'Team': rec['Team'],
                'team': rec['Team'],  # duplicate lowercase key for frontend consistency
                'Season': rec['Season'],
                'Season_State': rec['Season_State'],
                'Strength': rec['Strength'],
                'GP': len(rec['games']),
                'CF': CF,'CA': CA,'CF%': cf_pct,
                'FF': FF,'FA': FA,'FF%': ff_pct,
                'SF': SF,'SA': SA,'SF%': sf_pct,
                'GF': GF,'GA': GA,'GF%': gf_pct,
                'xGF': xGF,'xGA': xGA,'xGF%': xgf_pct,
                'Sh%': sh_pct,
                'Sv%': sv_pct,
                'PDO': pdo
            })
        # Sort: CF% desc then GF% desc then Team name
        out.sort(key=lambda r: (-(r['CF%'] if r['CF%'] is not None else -1), -(r['GF%'] if r['GF%'] is not None else -1), r['Team']))
        return out


report_store = ReportDataStore()