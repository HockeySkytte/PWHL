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

    def tables_skaters_individual(self, **kwargs):
        # Revert to the earlier minimal implementation (no strength slicing inside; rely on external filter) to remove inflation root cause.
        self.load()
        rows = self._apply_common_filters(self.rows, row_strength_independent=True, **kwargs)
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
        # First pass: shooting / goal / shot-based events
        for r in rows:
            shooter=r.get('shooter')
            if not shooter or shooter in goalie_names: continue
            team=r['team_for']
            s=ensure_player(shooter, team)
            s['GP'].add(r['game_id'])
            if r['is_goal']:
                s['G']+=1
                if r.get('assist1') and r['assist1'] not in goalie_names:
                    ensure_player(r['assist1'], team)['GP'].add(r['game_id'])
                if r.get('assist2') and r['assist2'] not in goalie_names:
                    ensure_player(r['assist2'], team)['GP'].add(r['game_id'])
            if r['is_shot']: s['Shots']+=1
            if r['is_miss']: s['Misses']+=1
            if r['is_block']: s['Shots_in_block']+=1
        # Second pass: assists + penalties
        for r in rows:
            if r['event']=='Goal':
                for a_field in ('assist1','assist2'):
                    a=r.get(a_field)
                    if a and a not in goalie_names:
                        ap=ensure_player(a, r['team_for'])
                        ap['A']+=1
                        ap['GP'].add(r['game_id'])
            if r['event']=='Penalty':
                p=r.get('shooter')
                if p and p not in goalie_names:
                    pp=ensure_player(p, r['team_for'])
                    pp['PEN_taken']+=1
                    pp['GP'].add(r['game_id'])
        # On-ice GF/GA (unfiltered aside from initial filters) – keeps raw totals
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
                sp=ensure_player(p, shooting_team); sp['GF']+=1; sp['GP'].add(gid)
            for p in against_players:
                if p in goalie_names: continue
                ap=ensure_player(p, r['team_against']); ap['GA']+=1; ap['GP'].add(gid)
        out=[]
        for rec in stats.values():
            rec['GP']=len(rec['GP'])
            rec['P']=rec['G']+rec['A']
            rec['Sh%']=round(rec['G']/rec['Shots']*100,1) if rec['Shots']>0 else 0
            total_goals=rec['GF']+rec['GA']
            rec['GF%']=round(rec['GF']/total_goals*100,1) if total_goals>0 else None
            total_toi_secs=sum(secs for (gid,name),secs in self.toi_lookup.items() if name==rec['player'])
            rec['TOI']=round(total_toi_secs/60,1) if total_toi_secs else 0.0
            rec['Season']=season_filter
            rec['Season_State']=season_state_filter
            rec['Strength']=strength_filter
            out.append(rec)
        out.sort(key=lambda x:(-x['P'],-x['G'],x['player']))
        return out
        

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
        requested_strength = kwargs.get('strength','All')
        # Always pull all strengths for internal aggregation to avoid double counting logic
        k_all = dict(kwargs)
        k_all['strength'] = 'All'
        rows = self._apply_common_filters(self.rows, row_strength_independent=True, **k_all)
        season_filter = kwargs.get('season','All')
        season_state_filter = kwargs.get('season_state','All')
        goalie_names = {r['goalie'] for r in rows if r.get('goalie')}

        # Structure: stats[player] = {'team':team,'GP':set(),'tot':{metrics}, 'by':{strength_class: metrics}}
        def blank_metrics():
            return {'G':0,'A':0,'Shots':0,'Misses':0,'Shots_in_block':0,'Blocks':0,'GF':0,'GA':0,'PEN_taken':0,'PEN_drawn':0}
        stats: Dict[str, Dict[str, Any]] = {}

        def ensure(player: str, team: str):
            if player not in stats:
                stats[player] = {'player':player,'team':team,'GP':set(),'tot':blank_metrics(),'by':{'5v5':blank_metrics(),'EV':blank_metrics(),'PP':blank_metrics(),'SH':blank_metrics()}}
            return stats[player]

        # Helper to map raw row strength to classification from perspective of team_for or opponent
        def row_class_for_team(row, team, is_for: bool):
            return self._classify_strength(row['strength'], team, is_for)

        # Iterate events once
        for r in rows:
            gid = r['game_id']
            shooter = r.get('shooter')
            team_for = r['team_for']
            if shooter and shooter not in goalie_names:
                rec = ensure(shooter, team_for)
                rec['GP'].add(gid)
                cls = row_class_for_team(r, team_for, True)
                # Offensive individual events
                if r['is_goal']:
                    rec['tot']['G'] += 1
                    rec['by'][cls]['G'] += 1
                if r['is_shot']:
                    rec['tot']['Shots'] += 1; rec['by'][cls]['Shots'] += 1
                if r['is_miss']:
                    rec['tot']['Misses'] += 1; rec['by'][cls]['Misses'] += 1
                if r['is_block']:
                    rec['tot']['Shots_in_block'] += 1; rec['by'][cls]['Shots_in_block'] += 1
            # Assists
            if r['event']=='Goal':
                for a_field in ('assist1','assist2'):
                    a = r.get(a_field)
                    if a and a not in goalie_names:
                        rec = ensure(a, team_for)
                        rec['GP'].add(gid)
                        cls = row_class_for_team(r, team_for, True)
                        rec['tot']['A'] += 1
                        rec['by'][cls]['A'] += 1
            # Penalties (taken only for now)
            if r['event']=='Penalty':
                pen_player = r.get('shooter')
                if pen_player and pen_player not in goalie_names:
                    rec = ensure(pen_player, team_for)
                    rec['GP'].add(gid)
                    cls = row_class_for_team(r, team_for, True)
                    rec['tot']['PEN_taken'] += 1
                    rec['by'][cls]['PEN_taken'] += 1
        # On-ice GF/GA
        for r in rows:
            if r['event']!='Goal':
                continue
            gid = r['game_id']
            meta = self.game_meta.get(gid, {})
            home = meta.get('home_team')
            shooting_team = r['team_for']
            shooter = r.get('shooter')
            if shooting_team==home:
                on_for = list(r.get('on_ice_home') or [])
                on_against = list(r.get('on_ice_away') or [])
            else:
                on_for = list(r.get('on_ice_away') or [])
                on_against = list(r.get('on_ice_home') or [])
            if shooter and shooter not in on_for and shooter not in goalie_names:
                on_for.append(shooter)
            # For side scoring
            cls_for = row_class_for_team(r, shooting_team, True)
            for p in on_for:
                if p in goalie_names: continue
                rec = ensure(p, shooting_team)
                rec['GP'].add(gid)
                rec['tot']['GF'] += 1
                rec['by'][cls_for]['GF'] += 1
            # Against side
            for p in on_against:
                if p in goalie_names: continue
                opp_team = r['team_against']
                cls_against = row_class_for_team(r, opp_team, False)
                rec = ensure(p, opp_team)
                rec['GP'].add(gid)
                rec['tot']['GA'] += 1
                rec['by'][cls_against]['GA'] += 1

        # Prepare output selecting requested strength subset
        def select_metrics(rec):
            if requested_strength=='All':
                return rec['tot']
            if requested_strength=='EV':
                # Combine 5v5 + EV buckets
                ev = blank_metrics()
                for k in ('5v5','EV'):
                    b = rec['by'][k]
                    for m,v in b.items():
                        ev[m]+=v
                return ev
            # direct bucket (PP, SH, 5v5)
            if requested_strength in rec['by']:
                return rec['by'][requested_strength]
            return blank_metrics()

        out=[]
        for rec in stats.values():
            metrics = select_metrics(rec)
            G = metrics['G']; A = metrics['A']
            Shots = metrics['Shots']; Misses=metrics['Misses']; Sib=metrics['Shots_in_block']
            GF=metrics['GF']; GA=metrics['GA']
            P = G + A
            Shp = round(G / Shots * 100,1) if Shots>0 else 0
            total_goals = GF + GA
            GFp = round(GF/total_goals*100,1) if total_goals>0 else None
            total_toi_secs = sum(secs for (gid,name),secs in self.toi_lookup.items() if name==rec['player'])
            out.append({
                'player': rec['player'],
                'team': rec['team'],
                'GP': len(rec['GP']),
                'TOI': round(total_toi_secs/60,1) if total_toi_secs else 0.0,
                'G': G,
                'A': A,
                'P': P,
                'Shots': Shots,
                'Misses': Misses,
                'Shots_in_block': Sib,
                'Blocks': 0,
                'GF': GF,
                'GA': GA,
                'GF%': GFp,
                'PEN_taken': metrics['PEN_taken'],
                'PEN_drawn': 0,
                'Sh%': Shp,
                'Season': season_filter,
                'Season_State': season_state_filter,
                'Strength': requested_strength
            })
        out.sort(key=lambda x:(-x['P'],-x['G'],x['player']))
        return out

    def tables_skaters_onice(self, **kwargs):
        return []

    def tables_goalies(self, **kwargs):
        return []

    def tables_teams(self, **kwargs):
        return []


report_store = ReportDataStore()