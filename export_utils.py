from __future__ import annotations
from typing import Any, Dict, List, Tuple
import io
import csv
import os
import json
import math


def csv_escape(val: Any) -> str:
    s = "" if val is None else str(val)
    if any(c in s for c in [',', '"', '\n']):
        return '"' + s.replace('"', '""') + '"'
    return s


def normalize_key(s: Any) -> str:
    return str(s or '').strip().lower().replace(' ', '_').replace('-', '_')


def prettify_event_label(k: str) -> str:
    mapping = {
        'shot': 'Shot',
        'goal': 'Goal',
        'block': 'Block',
        'blocked_shot': 'Block',
        'penalty': 'Penalty',
        'so_goal': 'SO Goal',
        'so_miss': 'SO Miss',
        'hit': 'Hit',
        'faceoff': 'Faceoff',
        'giveaway': 'Giveaway',
        'takeaway': 'Takeaway',
        'stoppage': 'Stoppage',
        'period_start': 'Period Start',
        'period_end': 'Period End',
        'shootout': 'Shootout',
    }
    k = normalize_key(k)
    if k in mapping:
        return mapping[k]
    return ' '.join(w.capitalize() for w in k.replace('_', ' ').split())


def toi_to_seconds(val: Any) -> Any:
    if val is None or val == '':
        return ''
    if isinstance(val, (int, float)):
        try:
            return int(val)
        except Exception:
            return ''
    s = str(val)
    if ':' in s:
        parts = s.split(':')
        try:
            mm = int(parts[0] or 0)
            ss = int(parts[1] or 0)
            return mm * 60 + ss
        except Exception:
            return ''
    try:
        return int(float(s))
    except Exception:
        return ''


def normalize_game_date(date: Any, season_year: Any, full_date: Any) -> str:
    # Prefer explicit full_date if present, else fallback to provided date string
    if full_date:
        return str(full_date)
    if date:
        return str(date)
    return ''


def csv_escape_list(items: List[Dict[str, Any]]) -> Tuple[str, str]:
    nos: List[str] = []
    names: List[str] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        no = it.get('jerseyNumber') or it.get('id') or ''
        name = ((it.get('firstName', '') + ' ' + it.get('lastName', '')).strip())
        nos.append(str(no))
        names.append(name)
    return ' '.join(nos), ' | '.join(names)


def generate_lineups_csv(
    game: Dict[str, Any],
    summary: Dict[str, Any],
    team_color_by_name: Dict[str, str] | None = None,
    team_color_by_id: Dict[str, str] | None = None,
) -> str:
    headers = ['Number', 'Name', 'Line', 'Venue', 'Team', 'Team Color', 'Game ID', 'Date', 'Competition', 'Season', 'State', 'TOI']
    out = io.StringIO()
    writer = csv.writer(out, lineterminator='\n')
    writer.writerow(headers)

    game_id = game.get('game_id')
    game_date = normalize_game_date(game.get('date'), game.get('season_year'), game.get('full_date',''))
    season_year = game.get('season_year')
    season_state = game.get('season_state')

    # Robust color lookup by name (exact, case-insensitive, accent-folded), then by id
    def resolve_team_color(team_name: str, team_id: str) -> str:
        if not team_name:
            team_name = ''
        # direct exact match
        if team_color_by_name and team_name in team_color_by_name:
            return team_color_by_name[team_name]
        # case-insensitive match
        if team_color_by_name:
            tl = team_name.lower()
            for k, v in team_color_by_name.items():
                if k.lower() == tl and v:
                    return v
        # accent-folded match
        try:
            import unicodedata
            def fold(s: str) -> str:
                s2 = unicodedata.normalize('NFD', s)
                return ''.join(ch for ch in s2 if unicodedata.category(ch) != 'Mn').lower()
            if team_color_by_name:
                tf = fold(team_name)
                for k, v in team_color_by_name.items():
                    if fold(k) == tf and v:
                        return v
        except Exception:
            pass
        # id-based fallback
        if team_color_by_id and team_id and team_id in team_color_by_id:
            return team_color_by_id[team_id]
        return ''

    def add_players(team_key: str, is_home: bool):
        team = summary.get(team_key) or {}
        # Always use schedule-derived team names to match Teams.csv keys precisely
        team_name = (game.get('home_team') if is_home else game.get('away_team')) or team.get('name') or ''
        tid = str((game.get('home_team_id') if is_home else game.get('away_team_id')) or '')
        team_color = resolve_team_color(team_name, tid)

        for grp in ('goalies', 'skaters'):
            for p in team.get(grp, []) or []:
                info = p.get('info', {})
                stats = p.get('stats', {})
                number = info.get('jerseyNumber') or p.get('jersey') or ''
                name = (info.get('firstName', '') + ' ' + info.get('lastName', '')).strip() or p.get('name', '')
                line = info.get('position') or p.get('position') or ''
                venue = 'Home' if is_home else 'Away'
                toi = toi_to_seconds(stats.get('timeOnIce') or stats.get('toi'))

                writer.writerow([
                    number,
                    name,
                    line,
                    venue,
                    team_name,
                    team_color,
                    str(game_id or ''),
                    game_date,
                    'PWHL',
                    season_year or '',
                    season_state or '',
                    '' if toi == '' else str(toi)
                ])

    add_players('homeTeam', True)
    add_players('visitingTeam', False)
    return out.getvalue()


def generate_pbp_csv(
    game: Dict[str, Any],
    pbp: List[Dict[str, Any]],
    summary: Dict[str, Any] | None = None,
    teams_meta: Dict[str, Dict[str, str]] | None = None,
) -> str:
    """Generate Play-by-Play CSV with logic matching the in-browser export exactly.
    Includes:
    - Event filtering and shot+goal merge at identical timestamps
    - Shootout team inference and SO_goal/SO_miss mapping
    - Strength computation (min 3 skaters, OT rules, queued penalties)
    - Coordinate normalization and player list joiners
    """
    headers = ['id','timestamp','event','team','venue','team_home','team_away','period','perspective','strength','p1_no','p1_name','p2_no','p2_name','p3_no','p3_name','g_no','goalie_name','home_line','home_players','home_players_names','away_line','away_players','away_players_names','x','y','xG','ScoreState','BoxID','game_id','game_date','competition','season','state']
    out = io.StringIO()
    writer = csv.writer(out, lineterminator='\n')
    writer.writerow(headers)

    game_id = game.get('game_id')
    game_date = normalize_game_date(game.get('date'), game.get('season_year'), game.get('full_date',''))

    # Helpers mirroring game.html
    # Build helpers for team codes/names
    name_to_code = (teams_meta or {}).get('name_to_code', {}) if isinstance(teams_meta, dict) else {}
    code_to_name = (teams_meta or {}).get('code_to_name', {}) if isinstance(teams_meta, dict) else {}

    # Extract numeric logo IDs from logo URLs to map to schedule names (e.g., .../1_5.png -> '1' -> 'Boston Fleet')
    logo_id_to_name: Dict[str, str] = {}
    def extract_logo_id(url: Any) -> str:
        s = str(url or '')
        try:
            base = s.rsplit('/', 1)[-1]
            head = base.split('_', 1)[0]
            return ''.join(ch for ch in head if ch.isdigit())
        except Exception:
            return ''
    hn = str(game.get('home_team') or '')
    an = str(game.get('away_team') or '')
    hid = extract_logo_id(game.get('home_team_logo'))
    aid = extract_logo_id(game.get('away_team_logo'))
    if hid:
        logo_id_to_name[str(hid)] = hn
    if aid:
        logo_id_to_name[str(aid)] = an

    def get_team_ids() -> Tuple[str, str]:
        home = str(game.get('home_team_id') or '')
        away = str(game.get('away_team_id') or '')
        return home, away

    def get_team_name_by_id(tid: str) -> str:
        home, away = get_team_ids()
        if tid and tid == str(home):
            return str(game.get('home_team') or '')
        if tid and tid == str(away):
            return str(game.get('away_team') or '')
        return ''

    def key_for(ev: Dict[str, Any]) -> str:
        d = ev.get('details') or {}
        period = d.get('period')
        period_id = ''
        if isinstance(period, dict):
            period_id = str(period.get('id') or period.get('shortName') or '')
        elif period is not None:
            period_id = str(period)
        t = d.get('time') or ''
        return f"{period_id}|{t}"

    def boolish(v: Any) -> bool:
        if v is True or v == 1:
            return True
        s = str(v).lower()
        return s in ('1','true','yes','y','t')

    # Build lineup index to help resolve shootout team ids
    def normalize_name(s: str) -> str:
        if not s:
            return ''
        try:
            # Basic ASCII fold by ignore accent combining marks
            import unicodedata
            s2 = unicodedata.normalize('NFD', s)
            s2 = ''.join(ch for ch in s2 if unicodedata.category(ch) != 'Mn')
        except Exception:
            s2 = s
        return s2.lower().replace('\u2019',' ').replace("'", ' ').replace('-', ' ').replace('`',' ').strip()

    lineup_index = {
        'ids': {},           # playerId -> teamId
        'names': {},         # full name -> teamId
        'namesLast': {},     # last name -> set(teamId)
        'homeNumbers': set(),
        'awayNumbers': set(),
        'homeId': str(game.get('home_team_id') or ''),
        'awayId': str(game.get('away_team_id') or ''),
    }
    if isinstance(summary, dict):
        def add_player(p: Dict[str, Any], team_id: str, is_home: bool):
            if not isinstance(p, dict):
                return
            info = p.get('info') or {}
            pid = str(info.get('id') or info.get('playerId') or info.get('playerID') or p.get('id') or '')
            if pid:
                # Hardcoded override per app: id 31 -> Montréal Victoire (team 3)
                if pid == '31':
                    lineup_index['ids'][pid] = '3'
                else:
                    lineup_index['ids'][pid] = str(team_id)
            first = str(info.get('firstName') or '').strip()
            last = str(info.get('lastName') or '').strip()
            full = (first + ' ' + last).strip() or str(p.get('name') or '').strip()
            nf = normalize_name(full)
            nl = normalize_name(last)
            if nf:
                lineup_index['names'][nf] = str(team_id)
            if nl:
                s = lineup_index['namesLast'].setdefault(nl, set())
                s.add(str(team_id))
            num = str(info.get('jerseyNumber') or p.get('jersey') or '').strip()
            if num:
                if is_home:
                    lineup_index['homeNumbers'].add(num)
                else:
                    lineup_index['awayNumbers'].add(num)

        for side, is_home in (('homeTeam', True), ('visitingTeam', False)):
            team = summary.get(side) or {}
            for grp in ('goalies','skaters'):
                for p in team.get(grp, []) or []:
                    add_player(p, lineup_index['homeId'] if is_home else lineup_index['awayId'], is_home)

    def get_player_team_from_lineups(player: Any) -> str:
        idx = lineup_index
        if not player:
            return ''
        if isinstance(player, dict):
            pid = str(player.get('id') or player.get('playerId') or player.get('playerID') or '').strip()
            if pid == '31':
                return '3'
            if pid and pid in idx['ids']:
                return idx['ids'][pid]
            raw_name = player.get('name') or player.get('fullName') or f"{player.get('firstName','')} {player.get('lastName','')}".strip()
            name_key = normalize_name(raw_name)
            if name_key and name_key in idx['names']:
                return idx['names'][name_key]
            last_key = normalize_name(player.get('lastName') or (str(raw_name).split(' ').pop() if raw_name else ''))
            if last_key and last_key in idx['namesLast'] and len(idx['namesLast'][last_key]) == 1:
                return next(iter(idx['namesLast'][last_key]))
            num = str(player.get('jerseyNumber') or player.get('jersey') or '').strip()
            if num:
                if num in idx['homeNumbers'] and num not in idx['awayNumbers']:
                    return idx['homeId']
                if num in idx['awayNumbers'] and num not in idx['homeNumbers']:
                    return idx['awayId']
        else:
            s = str(player).strip()
            name_key = normalize_name(s)
            if name_key and name_key in lineup_index['names']:
                return lineup_index['names'][name_key]
            last_key = normalize_name(s.split(' ').pop())
            if last_key and last_key in lineup_index['namesLast'] and len(lineup_index['namesLast'][last_key]) == 1:
                return next(iter(lineup_index['namesLast'][last_key]))
            if s.isdigit():
                if s in lineup_index['homeNumbers'] and s not in lineup_index['awayNumbers']:
                    return lineup_index['homeId']
                if s in lineup_index['awayNumbers'] and s not in lineup_index['homeNumbers']:
                    return lineup_index['awayId']
        return ''

    def resolve_shootout_team(details: Dict[str, Any]) -> str:
        home_id, away_id = get_team_ids()
        # 1) direct team id on event
        direct = str(details.get('team', {}).get('id') or details.get('teamId') or details.get('team_id') or '').strip()
        if direct in (home_id, away_id):
            return direct
        # 2) via shooter/scoredBy object
        shooter = details.get('shooter') or details.get('player') or details.get('scoredBy')
        via = get_player_team_from_lineups(shooter)
        if via:
            return via
        # 3) if goalie present, return opponent team
        goalie = details.get('goalie')
        g_team = get_player_team_from_lineups(goalie)
        if g_team:
            return home_id if g_team == away_id else away_id
        return ''

    # Collect mapping from PWHL numeric team id -> team abbreviation (e.g., 6 -> 'TOR')
    id_to_abbr: Dict[str, str] = {}
    for ev in pbp or []:
        d0 = ev.get('details') or {}
        t = d0.get('team') or {}
        if isinstance(t, dict):
            tid = str(t.get('id') or '').strip()
            ab = str(t.get('abbreviation') or '').strip()
            if tid and ab:
                id_to_abbr[tid] = ab
        at = d0.get('againstTeam') or {}
        if isinstance(at, dict):
            tid = str(at.get('id') or '').strip()
            ab = str(at.get('abbreviation') or '').strip()
            if tid and ab:
                id_to_abbr[tid] = ab

    # Build merged/filtered final events
    events_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for ev in pbp or []:
        k = key_for(ev)
        events_by_key.setdefault(k, []).append(ev)

    removed_types = {'goalie_change','goalie-change','hit','faceoff'}
    consumed: set[int] = set()
    final_events: List[Dict[str, Any]] = []

    # Flatten with merge rules
    for idx, ev in enumerate(pbp or []):
        if idx in consumed:
            continue
        etype = str(ev.get('event') or '').lower()
        if etype in removed_types:
            continue
        d = ev.get('details') or {}
        key = key_for(ev)

        if etype == 'goal':
            goal_team_id = str((d.get('team') or {}).get('id') or '')
            # Find matching shot by same team at same timestamp
            shot_ev = None
            for cand in events_by_key.get(key, []) or []:
                if str(cand.get('event') or '').lower() != 'shot':
                    continue
                cd = cand.get('details') or {}
                if str(cd.get('shooterTeamId') or '') == goal_team_id:
                    shot_ev = cand
                    break
            if shot_ev is not None:
                try:
                    consumed.add((pbp or []).index(shot_ev))
                except Exception:
                    pass
            shotd = shot_ev.get('details') if isinstance(shot_ev, dict) else {}
            final_events.append({
                **ev,
                '_computedTeamId': goal_team_id,
                '_mergedScorer': (shotd or {}).get('shooter') or d.get('scoredBy') or d.get('scorer') or d.get('player') or None,
                '_mergedAssists': list(d.get('assists') or [])[:2],
                '_mergedGoalie': (shotd or {}).get('goalie') or d.get('goalie') or None,
                '_x': d.get('xLocation') if d.get('xLocation') is not None else (shotd or {}).get('xLocation'),
                '_y': d.get('yLocation') if d.get('yLocation') is not None else (shotd or {}).get('yLocation'),
            })
            continue

        if etype == 'shot':
            shooter_team_id = str(d.get('shooterTeamId') or d.get('teamId') or d.get('team_id') or '')
            has_matching_goal = False
            for g in events_by_key.get(key, []) or []:
                if str(g.get('event') or '').lower() != 'goal':
                    continue
                gd = g.get('details') or {}
                if str((gd.get('team') or {}).get('id') or '') == shooter_team_id:
                    has_matching_goal = True
                    break
            if has_matching_goal:
                continue
            final_events.append({ **ev, '_computedTeamId': shooter_team_id, '_x': d.get('xLocation'), '_y': d.get('yLocation') })
            continue

        if etype == 'shootout':
            team_resolved = resolve_shootout_team(d)
            so_goal = boolish(d.get('isGoal') or (d.get('properties') or {}).get('isGoal'))
            final_events.append({ **ev, '_computedTeamId': str(team_resolved or ''), '_overrideEvent': 'SO_goal' if so_goal else 'SO_miss' })
            continue

        if etype == 'penalty':
            commit_id = str((d.get('againstTeam') or {}).get('id') or '')
            if not commit_id:
                commit_id = str(d.get('teamId') or d.get('team_id') or '')
            final_events.append({ **ev, '_computedTeamId': commit_id })
            continue

        # Treat blocked shots like other shot attempts for strength orientation:
        # use the SHOOTING team (typically details.againstTeam.id or shooterTeamId)
        if etype in ('block', 'blocked-shot', 'blocked_shot'):
            shooter_team_id = ''
            at = d.get('againstTeam') or {}
            if isinstance(at, dict):
                shooter_team_id = str(at.get('id') or '')
            if not shooter_team_id:
                shooter_team_id = str(d.get('shooterTeamId') or '')
            if not shooter_team_id:
                # Try to infer via goalie team (opponent of shooter)
                try:
                    g_team = get_player_team_from_lineups(d.get('goalie')) if d.get('goalie') else ''
                except Exception:
                    g_team = ''
                if g_team:
                    home_id, away_id = get_team_ids()
                    shooter_team_id = home_id if str(g_team) == str(away_id) else (away_id if str(g_team) == str(home_id) else '')
            computed = shooter_team_id or str(d.get('teamId') or d.get('team_id') or (d.get('team') or {}).get('id') or '')
            final_events.append({ **ev, '_computedTeamId': computed, '_x': d.get('xLocation'), '_y': d.get('yLocation') })
            continue

        # Default: carry team id from details
        computed = str(d.get('teamId') or d.get('team_id') or d.get('shooterTeamId') or d.get('scorerTeamId') or (d.get('team') or {}).get('id') or '')
        final_events.append({ **ev, '_computedTeamId': computed })

    # Ensure shootout events alternate when team missing
    def fix_shootout_teams(events: List[Dict[str, Any]]):
        home_id, away_id = get_team_ids()
        idxs = []
        for i, ev in enumerate(events):
            label = str(ev.get('_overrideEvent') or ev.get('event') or '').upper()
            if label in ('SO_GOAL','SO_MISS') or str(ev.get('event') or '').lower() == 'shootout':
                idxs.append(i)
        if not idxs:
            return
        # determine alternation
        start = ''
        second = ''
        for i in idxs:
            t = str(events[i].get('_computedTeamId') or '').strip()
            if t in (home_id, away_id):
                if not start:
                    start = t
                elif not second and t != start:
                    second = t
                    break
        if not start:
            start = away_id or home_id
        if not second:
            second = home_id if start == away_id else away_id
        current = start
        def other(t: str) -> str:
            return home_id if t == away_id else away_id
        for i in idxs:
            t = str(events[i].get('_computedTeamId') or '').strip()
            if not t or t not in (home_id, away_id):
                events[i]['_computedTeamId'] = current
            elif t != current:
                current = t
            current = other(current)

    fix_shootout_teams(final_events)

    # Strength computation matching app
    def compute_strengths(events: List[Dict[str, Any]]) -> List[str]:
        def to_seconds(ts: str) -> int:
            if not ts:
                return 0
            parts = str(ts).split(':')
            mm = int(parts[0] or 0)
            ss = int(parts[1] or 0)
            return mm*60+ss
        def period_index(d: Dict[str, Any]) -> int:
            p = d.get('period')
            v = (p.get('id') if isinstance(p, dict) else p) if p is not None else ''
            s = str((p.get('shortName') if isinstance(p, dict) else v) or '').upper()
            if s in ('1','2','3'):
                return int(s)
            if s in ('4','OT') or s.startswith('OT'):
                return 4
            if s in ('SO','SHOOTOUT'):
                return 5
            try:
                return int(s)
            except Exception:
                return 1
        is_regular = str(game.get('season_state') or '').lower() == 'regular season'

        order = [
            {'e': e, 'i': i, 'pi': period_index(e.get('details') or {}), 'ts': to_seconds((e.get('details') or {}).get('time') or '0:00')}
            for i, e in enumerate(events)
        ]
        order.sort(key=lambda x: (x['pi'], x['ts'], x['i']))

        home_id, away_id = get_team_ids()
        active: List[Dict[str, Any]] = []  # {teamId, duration, endPi, endTs}
        pending: List[Dict[str, Any]] = []
        results: Dict[int, str] = {}

        def compute_end(pi: int, ts: int, dur: int) -> Tuple[int, int]:
            end_pi, end_ts = pi, ts + dur
            while end_ts >= 1200:
                end_ts -= 1200
                end_pi += 1
            return end_pi, end_ts

        def schedule_follow_on(pi: int, ts: int, pen_entry: Dict[str, Any]):
            follow = pen_entry.get('follow_on') or []
            follow_flags = pen_entry.get('follow_on_releasable') or []
            if not follow:
                return
            seg_dur = follow[0]
            rel = follow_flags[0] if follow_flags else True
            remaining = follow[1:]
            remaining_flags = follow_flags[1:] if follow_flags else []
            team_id = pen_entry.get('teamId')
            entry = {
                'teamId': team_id,
                'duration': seg_dur,
                'releasable': rel,
                'follow_on': remaining,
                'follow_on_releasable': remaining_flags,
            }
            count = sum(1 for p in active if p['teamId'] == team_id)
            if count < 2:
                end_pi, end_ts = compute_end(pi, ts, seg_dur)
                entry['endPi'] = end_pi
                entry['endTs'] = end_ts
                active.append(entry)
            else:
                pending.append(entry)

        def pop_expired(pi: int, ts: int):
            # remove expired and schedule follow-ons
            for j in range(len(active)-1, -1, -1):
                p = active[j]
                if pi > p['endPi'] or (pi == p['endPi'] and ts >= p['endTs']):
                    active.pop(j)
                    schedule_follow_on(pi, ts, p)
            if pending:
                counts: Dict[str, int] = {}
                for a in active:
                    counts[a['teamId']] = counts.get(a['teamId'], 0) + 1
                k = 0
                while k < len(pending):
                    pen = pending[k]
                    c = counts.get(pen['teamId'], 0)
                    if c < 2:
                        end_pi, end_ts = compute_end(pi, ts, pen['duration'])
                        pen['endPi'] = end_pi
                        pen['endTs'] = end_ts
                        active.append(pen)
                        counts[pen['teamId']] = c + 1
                        pending.pop(k)
                    else:
                        k += 1

        def add_penalty(pi: int, ts: int, minutes: Any, team_id: str):
            try:
                minutes_i = int(minutes)
            except Exception:
                minutes_i = 2
            team_id = str(team_id or '')
            if minutes_i == 4:
                segments = [120, 120]
                releasable_flags = [True, True]
            elif minutes_i == 5:
                segments = [300]
                releasable_flags = [False]
            elif minutes_i == 2:
                segments = [120]
                releasable_flags = [True]
            else:
                dur = minutes_i * 60
                segments = [dur]
                releasable_flags = [dur <= 120]

            def push(seg_index: int, start_pi: int, start_ts: int):
                seg_dur = segments[seg_index]
                rel = releasable_flags[seg_index]
                entry = {
                    'teamId': team_id,
                    'duration': seg_dur,
                    'releasable': rel,
                    'follow_on': [] if seg_index == len(segments)-1 else segments[seg_index+1:],
                    'follow_on_releasable': [] if seg_index == len(releasable_flags)-1 else releasable_flags[seg_index+1:],
                }
                count = sum(1 for p in active if p['teamId'] == team_id)
                if count < 2:
                    end_pi, end_ts = compute_end(start_pi, start_ts, seg_dur)
                    entry['endPi'] = end_pi
                    entry['endTs'] = end_ts
                    active.append(entry)
                else:
                    pending.append(entry)

            push(0, pi, ts)

        def count_against(team_id: str) -> int:
            return sum(1 for p in active if p['teamId'] == str(team_id or ''))

        i = 0
        n = len(order)
        while i < n:
            group_start = i
            pi = order[i]['pi']
            ts = order[i]['ts']
            pop_expired(pi, ts)
            j = i + 1
            while j < n and order[j]['pi'] == pi and order[j]['ts'] == ts:
                j += 1

            # Emit strengths for this group
            for k in range(group_start, j):
                row = order[k]
                ev = row['e']
                d = ev.get('details') or {}
                et = str(ev.get('event') or '').lower()
                team_id = str(ev.get('_computedTeamId') or '')
                home_base = 5
                away_base = 5
                is_ot = (pi == 4)
                if is_ot and is_regular:
                    home_base = 3
                    away_base = 3
                # penalty shot or shootout
                pt = str(d.get('penaltyType') or '').lower()
                if et == 'shootout' or 'penalty shot' in pt or boolish(d.get('isPenaltyShot')):
                    results[row['i']] = '1v0'
                    continue
                home_p = count_against(home_id)
                away_p = count_against(away_id)
                if is_ot and is_regular:
                    home_base = min(5, 3 + away_p)
                    away_base = min(5, 3 + home_p)
                else:
                    home_base = max(3, home_base - home_p)
                    away_base = max(3, away_base - away_p)
                is_home = (team_id and team_id == str(home_id))
                results[row['i']] = f"{home_base}v{away_base}" if is_home else f"{away_base}v{home_base}"

            # Apply changes after group
            # Collect all penalty events at this exact timestamp for coincidental evaluation
            penalty_events: List[Dict[str, Any]] = []
            for k in range(group_start, j):
                row = order[k]
                ev = row['e']
                d = ev.get('details') or {}
                et = str(ev.get('event') or '').lower()
                if et != 'penalty':
                    continue
                pen_type = str(d.get('penaltyType') or '').lower()
                is_penalty_shot = ('penalty shot' in pen_type) or boolish(d.get('isPenaltyShot'))
                # Duration in minutes (fallback 2). Some feeds use 'minutes', others 'penaltyMinutes'.
                minutes_val = d.get('minutes') or d.get('penaltyMinutes') or 2
                try:
                    minutes_int = int(minutes_val)
                except Exception:
                    minutes_int = 2
                penalized_team = str((d.get('againstTeam') or {}).get('id') or ev.get('_computedTeamId') or '').strip()
                penalty_events.append({
                    'row': row,
                    'team': penalized_team,
                    'minutes': minutes_int,
                    'is_shot': is_penalty_shot,
                    'type': pen_type,
                })

            # Determine which penalties actually affect manpower:
            # 1. Ignore penalty shots (handled as 1v0 in strength emission above)
            # 2. Ignore misconducts / game misconducts (>=10 minutes) – they do not change on-ice strength
            # 3. Detect coincidental penalties at same timestamp (equal count & duration both teams) – no manpower change
            #    (Simplified: for each distinct duration, if counts for home & away are equal and >0, mark all as coincidental)
            # NOTE: Double-minors (4) treated as a single 4-minute manpower penalty. Early PP goal currently
            #       clears entire 4 (simplification versus real rule which would reduce to remaining 2). TODO for refinement.
            if penalty_events:
                # Build duration -> team -> count for manpower-eligible candidates first
                duration_team_counts: Dict[int, Dict[str, int]] = {}
                for pe in penalty_events:
                    if pe['is_shot']:
                        continue
                    if pe['minutes'] >= 10:  # misconduct
                        continue
                    if not pe['team']:
                        continue
                    dur = pe['minutes']
                    duration_team_counts.setdefault(dur, {})[pe['team']] = duration_team_counts.setdefault(dur, {}).get(pe['team'], 0) + 1

                coincidental_flags: Dict[int, bool] = {}  # index in penalty_events -> is coincidental
                for dur, team_counts in duration_team_counts.items():
                    if len(team_counts) < 2:
                        continue
                    counts_vals = list(team_counts.values())
                    # All counts equal? (e.g., 1 vs 1, 2 vs 2). If so, mark all those of this duration as coincidental.
                    if len(set(counts_vals)) == 1:
                        for idx, pe in enumerate(penalty_events):
                            if pe['minutes'] == dur and not pe['is_shot'] and pe['minutes'] < 10:
                                coincidental_flags[idx] = True

                # Apply manpower-changing penalties (not coincidental / not misconduct / not penalty shot)
                for idx, pe in enumerate(penalty_events):
                    if pe['is_shot']:
                        continue
                    if pe['minutes'] >= 10:  # misconduct unaffected
                        continue
                    if coincidental_flags.get(idx):
                        continue
                    if not pe['team']:
                        continue
                    add_penalty(pi, ts, pe['minutes'], pe['team'])
            # Expire one releasable segment on PP/SH goals
            for k in range(group_start, j):
                row = order[k]
                ev = row['e']
                d = ev.get('details') or {}
                et = str(ev.get('event') or '').lower()
                if et == 'goal':
                    team_id = str(ev.get('_computedTeamId') or '')
                    is_home = team_id == str(home_id)
                    is_pp = boolish((d.get('properties') or {}).get('isPowerPlay')) or boolish(d.get('isPowerPlay')) or boolish(d.get('is_power_play'))
                    is_sh = boolish((d.get('properties') or {}).get('isShortHanded')) or boolish(d.get('isShortHanded')) or boolish(d.get('is_short_handed'))
                    if is_pp or is_sh:
                        opp = str(away_id if is_home else home_id)
                        candidates = [idx_a for idx_a, p in enumerate(active) if p['teamId'] == opp and p.get('releasable')]
                        if candidates:
                            best = min(candidates, key=lambda ix: (active[ix]['endPi'], active[ix]['endTs']))
                            removed = active.pop(best)
                            schedule_follow_on(pi, ts, removed)

            i = j

        return [results.get(ii, '') for ii in range(len(events))]

    strengths = compute_strengths(final_events)

    # --- xG model helpers ---
    _XG_MODEL: Dict[str, Any] | None = None
    def load_xg_model() -> Dict[str, Any] | None:
        nonlocal _XG_MODEL
        if _XG_MODEL is not None:
            return _XG_MODEL
        try:
            path = os.path.join('models', 'xg_model.json')
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    _XG_MODEL = json.load(f)
            else:
                _XG_MODEL = None
        except Exception:
            _XG_MODEL = None
        return _XG_MODEL

    def strength_state_from(s: str) -> str:
        try:
            parts = str(s or '').lower().split('v')
            a = int(parts[0])
            b = int(parts[1])
            if a == b:
                return 'EV'
            return 'PP' if a > b else 'SH'
        except Exception:
            return 'EV'

    def clamp_score_state(s: Any) -> str:
        try:
            i = int(s)
        except Exception:
            i = 0
        if i < -2:
            i = -2
        if i > 2:
            i = 2
        return str(i)

    def xg_for(strength_str: str, score_state: str, box_id: str) -> str:
        model = load_xg_model()
        if not isinstance(model, dict):
            return ''
        md = model.get('model') or {}
        feats_list = md.get('features') or []
        coefs = md.get('coefficients') or {}
        intercept = float(md.get('intercept') or 0.0)
        # build feature keys
        s_state = strength_state_from(strength_str)
        sc = clamp_score_state(score_state)
        bx = box_id if isinstance(box_id, str) and box_id.startswith('O') else 'N_or_D'
        keys = [f'StrengthState_{s_state}', f'ScoreState_{sc}', f'BoxID_{bx}']
        z = intercept
        for k in keys:
            if k in coefs:
                try:
                    z += float(coefs[k])
                except Exception:
                    pass
        try:
            p = 1.0 / (1.0 + math.exp(-z))
            return f"{p:.4f}"
        except Exception:
            return ''

    # --- ScoreState tracking (running score before applying a goal) ---
    home_goals = 0
    away_goals = 0

    # --- Zone polygons (full half-rink sets) for BoxID ---
    # Coordinates are in normalized rink units: x in [-100,100], y in [-42.5,42.5]
    # Include Offensive (O**), Defensive (D**), and Neutral (N**) zones to ensure coverage.
    ZONES: List[Tuple[str, List[Tuple[float, float]]]] = []
    def add_zone(zone_id: str, coords: List[List[float]]):
        ZONES.append((zone_id, [(float(x), float(y)) for x,y in coords]))
    # Offensive zones
    add_zone('O01', [[89,-12.5],[100,-12.5],[100,-14],[99.9989034876783,-14.25],[99.99561369755,-14.5],[99.9901298698339,-14.75],[99.9824507372522,-15],[99.9725745235657,-15.25],[99.9604989415154,-15.5],[99.9462211901686,-15.75],[99.929737951659,-16],[99.9110453873137,-16.25],[99.8901391331568,-16.5],[99.8670142947755,-16.75],[99.8416654415368,-17],[99.814086600136,-17.25],[99.7842712474619,-17.5],[99.752212302756,-17.75],[99.7179021190449,-18],[99.6813324738203,-18.25],[99.6424945589406,-18.5],[99.6013789697232,-18.75],[99.5579756931964,-19],[99.5122740954747,-19.25],[99.4642629082191,-19.5],[99.4139302141422,-19.75],[99.3612634315101,-20],[99.306249297595,-20.25],[99.2488738510232,-20.5],[99.1891224129621,-20.75],[99.1269795670826,-21],[99.0624291382309,-21.25],[98.995454169735,-21.5],[98.9260368992678,-21.75],[98.8541587331799,-22],[98.7798002192098,-22.25],[98.7029410174709,-22.5],[98.6235598696041,-22.75],[98.5416345659799,-23],[98.4571419108184,-23.25],[98.3700576850888,-23.5],[98.2803566070357,-23.75],[98.188012290165,-24],[98.0929971985107,-24.25],[97.9952825989835,-24.5],[97.8948385105876,-24.75],[97.7916336502698,-25],[97.6856353751441,-25.25],[97.5768096208106,-25.5],[97.4651208354592,-25.75],[97.3505319094211,-26],[97.2330040997937,-26.25],[97.1124969497314,-26.5],[96.9889682019496,-26.75],[96.8623737059448,-27],[96.7326673183792,-27.25],[96.5998007960223,-27.5],[96.463723680573,-27.75],[96.3243831746128,-28],[96.1817240078565,-28.25],[96.0356882927706,-28.5],[95.8862153685233,-28.75],[95.7332416321053,-29],[95.5767003553228,-29.25],[95.4165214862028,-29.5],[95.2526314331697,-29.75],[95.0849528301415,-30],[94.9134042804544,-30.25],[94.7379000772445,-30.5],[94.5583498975967,-30.75],[94.3746584673957,-31],[94.1867251933813,-31.25],[93.994443758404,-31.5],[93.7977016752848,-31.75],[93.5963797939844,-32],[93.3903517559677,-32.25],[93.1794833886788,-32.5],[92.9636320318813,-32.75],[92.742645786248,-33],[92.516362672927,-33.25],[92.2846096908265,-33.5],[92.0472017559569,-33.75],[91.803940504247,-34],[91.5546129356814,-34.25],[91.2989898732233,-34.5],[91.036824204563,-34.75],[90.7678488679977,-35],[90.4917745353087,-35.25],[90.2082869338697,-35.5],[89.917043736713,-35.75],[89.6176709319934,-36],[89.3097585609688,-36.25],[89,-36.5],[89,-12.5]])
    add_zone('O02', [[89,-12.5],[100,-12.5],[100,12.5],[89,12.5],[89,-12.5]])
    add_zone('O03', [[89,12.5],[100,12.5],[100,14],[99.9989034876783,14.25],[99.99561369755,14.5],[99.9901298698339,14.75],[99.9824507372522,15],[99.9725745235657,15.25],[99.9604989415154,15.5],[99.9462211901686,15.75],[99.929737951659,16],[99.9110453873137,16.25],[99.8901391331568,16.5],[99.8670142947755,16.75],[99.8416654415368,17],[99.814086600136,17.25],[99.7842712474619,17.5],[99.752212302756,17.75],[99.7179021190449,18],[99.6813324738203,18.25],[99.6424945589406,18.5],[99.6013789697232,18.75],[99.5579756931964,19],[99.5122740954747,19.25],[99.4642629082191,19.5],[99.4139302141422,19.75],[99.3612634315101,20],[99.306249297595,20.25],[99.2488738510232,20.5],[99.1891224129621,20.75],[99.1269795670826,21],[99.0624291382309,21.25],[98.995454169735,21.5],[98.9260368992678,21.75],[98.8541587331799,22],[98.7798002192098,22.25],[98.7029410174709,22.5],[98.6235598696041,22.75],[98.5416345659799,23],[98.4571419108184,23.25],[98.3700576850888,23.5],[98.2803566070357,23.75],[98.188012290165,24],[98.0929971985107,24.25],[97.9952825989835,24.5],[97.8948385105876,24.75],[97.7916336502698,25],[97.6856353751441,25.25],[97.5768096208106,25.5],[97.4651208354592,25.75],[97.3505319094211,26],[97.2330040997937,26.25],[97.1124969497314,26.5],[96.9889682019496,26.75],[96.8623737059448,27],[96.7326673183792,27.25],[96.5998007960223,27.5],[96.463723680573,27.75],[96.3243831746128,28],[96.1817240078565,28.25],[96.0356882927706,28.5],[95.8862153685233,28.75],[95.7332416321053,29],[95.5767003553228,29.25],[95.4165214862028,29.5],[95.2526314331697,29.75],[95.0849528301415,30],[94.9134042804544,30.25],[94.7379000772445,30.5],[94.5583498975967,30.75],[94.3746584673957,31],[94.1867251933813,31.25],[93.994443758404,31.5],[93.7977016752848,31.75],[93.5963797939844,32],[93.3903517559677,32.25],[93.1794833886788,32.5],[92.9636320318813,32.75],[92.742645786248,33],[92.516362672927,33.25],[92.2846096908265,33.5],[92.0472017559569,33.75],[91.803940504247,34],[91.5546129356814,34.25],[91.2989898732233,34.5],[91.036824204563,34.75],[90.7678488679977,35],[90.4917745353087,35.25],[90.2082869338697,35.5],[89.917043736713,35.75],[89.6176709319934,36],[89.3097585609688,36.25],[89,36.5],[89,12.5]])
    add_zone('O04', [[89,-36.5],[89,-28],[60,-42.5],[71.5,-42.5],[75.2666297933298,-42.25],[76.8150729063673,-42],[77.9951905283833,-41.75],[78.9833147735479,-41.5],[79.847903928532,-41.25],[80.6241437954473,-41],[81.3329802196486,-40.75],[81.9880884817015,-40.5],[82.5989864402116,-40.25],[83.1726175299288,-40],[83.7142335003061,-39.75],[84.2279220613579,-39.5],[84.7169398878863,-39.25],[85.183932183404,-39],[85.6310827610626,-38.75],[86.060219778561,-38.5],[86.472892172189,-38.25],[86.8704261489394,-38],[87.2539677541882,-37.75],[87.6245154965971,-37.5],[87.9829457318769,-37.25],[88.3300326797068,-37],[88.6664644001029,-36.75],[89,-36.5]])
    add_zone('O05', [[89,-28],[89,-16],[73,-24],[73,-36],[89,-28]])
    add_zone('O06', [[89,-16],[89,-4],[73,-12],[73,-24],[89,-16]])
    add_zone('O07', [[89,-4],[89,4],[83,7],[83,-7],[89,-4]])
    add_zone('O08', [[89,4],[89,16],[73,24],[73,12],[89,4]])
    add_zone('O09', [[89,16],[89,28],[73,36],[73,24],[89,16]])
    add_zone('O10', [[89,36.5],[89,28],[60,42.5],[71.5,42.5],[75.2666297933298,42.25],[76.8150729063673,42],[77.9951905283833,41.75],[78.9833147735479,41.5],[79.847903928532,41.25],[80.6241437954473,41],[81.3329802196486,40.75],[81.9880884817015,40.5],[82.5989864402116,40.25],[83.1726175299288,40],[83.7142335003061,39.75],[84.2279220613579,39.5],[84.7169398878863,39.25],[85.183932183404,39],[85.6310827610626,38.75],[86.060219778561,38.5],[86.472892172189,38.25],[86.8704261489394,38],[87.2539677541882,37.75],[87.6245154965971,37.5],[87.9829457318769,37.25],[88.3300326797068,37],[88.6664644001029,36.75],[89,36.5]])
    add_zone('O11', [[83,-7],[83,7],[73,12],[73,-12],[83,-7]])
    add_zone('O12', [[73,-36],[73,-24],[57,-32],[57,-42.5],[60,-42.5],[73,-36]])
    add_zone('O13', [[73,-24],[73,-12],[57,-20],[57,-32],[73,-24]])
    add_zone('O14', [[73,-12],[73,0],[57,-8],[57,-20],[73,-12]])
    add_zone('O15', [[73,0],[57,8],[57,-8],[73,0]])
    add_zone('O16', [[73,0],[73,12],[57,20],[57,8],[73,0]])
    add_zone('O17', [[73,12],[73,24],[57,32],[57,20],[73,12]])
    add_zone('O18', [[73,24],[73,36],[60,42.5],[57,42.5],[57,32],[73,24]])
    add_zone('O19', [[57,-42.5],[57,-20],[41,-28],[41,-42.5],[57,-42.5]])
    add_zone('O20', [[57,-20],[57,-8],[41,-16],[41,-28],[57,-20]])
    add_zone('O21', [[57,-8],[57,8],[41,16],[41,-16],[57,-8]])
    add_zone('O22', [[57,8],[57,20],[41,28],[41,16],[57,8]])
    add_zone('O23', [[57,20],[57,42.5],[41,42.5],[41,28],[57,20]])
    add_zone('O24', [[41,-42.5],[41,-16],[25,-24],[25,-42.5],[41,-42.5]])
    add_zone('O25', [[41,-16],[41,16],[25,24],[25,-24],[41,-16]])
    add_zone('O26', [[41,16],[41,42.5],[25,42.5],[25,24],[41,16]])
    # Neutral zones
    add_zone('N01', [[25,-42.5],[25,-17.5],[0,-17.5],[0,-42.5],[25,-42.5]])
    add_zone('N02', [[25,-17.5],[25,17.5],[0,17.5],[0,-17.5],[25,-17.5]])
    add_zone('N03', [[25,17.5],[25,42.5],[0,42.5],[0,17.5],[25,17.5]])
    add_zone('N04', [[0,-42.5],[0,-17.5],[-25,-17.5],[-25,-42.5],[0,-42.5]])
    add_zone('N05', [[0,-17.5],[0,17.5],[-25,17.5],[-25,-17.5],[0,-17.5]])
    add_zone('N06', [[0,17.5],[0,42.5],[-25,42.5],[-25,17.5],[0,17.5]])
    # Defensive zones
    add_zone('D01', [[-89,12.5],[-100,12.5],[-100,14],[-99.9989034876783,14.25],[-99.99561369755,14.5],[-99.9901298698339,14.75],[-99.9824507372522,15],[-99.9725745235657,15.25],[-99.9604989415154,15.5],[-99.9462211901686,15.75],[-99.929737951659,16],[-99.9110453873137,16.25],[-99.8901391331568,16.5],[-99.8670142947755,16.75],[-99.8416654415368,17],[-99.814086600136,17.25],[-99.7842712474619,17.5],[-99.752212302756,17.75],[-99.7179021190449,18],[-99.6813324738203,18.25],[-99.6424945589406,18.5],[-99.6013789697232,18.75],[-99.5579756931964,19],[-99.5122740954747,19.25],[-99.4642629082191,19.5],[-99.4139302141422,19.75],[-99.3612634315101,20],[-99.306249297595,20.25],[-99.2488738510232,20.5],[-99.1891224129621,20.75],[-99.1269795670826,21],[-99.0624291382309,21.25],[-98.995454169735,21.5],[-98.9260368992678,21.75],[-98.8541587331799,22],[-98.7798002192098,22.25],[-98.7029410174709,22.5],[-98.6235598696041,22.75],[-98.5416345659799,23],[-98.4571419108184,23.25],[-98.3700576850888,23.5],[-98.2803566070357,23.75],[-98.188012290165,24],[-98.0929971985107,24.25],[-97.9952825989835,24.5],[-97.8948385105876,24.75],[-97.7916336502698,25],[-97.6856353751441,25.25],[-97.5768096208106,25.5],[-97.4651208354592,25.75],[-97.3505319094211,26],[-97.2330040997937,26.25],[-97.1124969497314,26.5],[-96.9889682019496,26.75],[-96.8623737059448,27],[-96.7326673183792,27.25],[-96.5998007960223,27.5],[-96.463723680573,27.75],[-96.3243831746128,28],[-96.1817240078565,28.25],[-96.0356882927706,28.5],[-95.8862153685233,28.75],[-95.7332416321053,29],[-95.5767003553228,29.25],[-95.4165214862028,29.5],[-95.2526314331697,29.75],[-95.0849528301415,30],[-94.9134042804544,30.25],[-94.7379000772445,30.5],[-94.5583498975967,30.75],[-94.3746584673957,31],[-94.1867251933813,31.25],[-93.994443758404,31.5],[-93.7977016752848,31.75],[-93.5963797939844,32],[-93.3903517559677,32.25],[-93.1794833886788,32.5],[-92.9636320318813,32.75],[-92.742645786248,33],[-92.516362672927,33.25],[-92.2846096908265,33.5],[-92.0472017559569,33.75],[-91.803940504247,34],[-91.5546129356814,34.25],[-91.2989898732233,34.5],[-91.036824204563,34.75],[-90.7678488679977,35],[-90.4917745353087,35.25],[-90.2082869338697,35.5],[-89.917043736713,35.75],[-89.6176709319934,36],[-89.3097585609688,36.25],[-89,36.5],[-89,12.5]])
    add_zone('D02', [[-89,12.5],[-100,12.5],[-100,-12.5],[-89,-12.5],[-89,12.5]])
    add_zone('D03', [[-89,-12.5],[-100,-12.5],[-100,-14],[-99.9989034876783,-14.25],[-99.99561369755,-14.5],[-99.9901298698339,-14.75],[-99.9824507372522,-15],[-99.9725745235657,-15.25],[-99.9604989415154,-15.5],[-99.9462211901686,-15.75],[-99.929737951659,-16],[-99.9110453873137,-16.25],[-99.8901391331568,-16.5],[-99.8670142947755,-16.75],[-99.8416654415368,-17],[-99.814086600136,-17.25],[-99.7842712474619,-17.5],[-99.752212302756,-17.75],[-99.7179021190449,-18],[-99.6813324738203,-18.25],[-99.6424945589406,-18.5],[-99.6013789697232,-18.75],[-99.5579756931964,-19],[-99.5122740954747,-19.25],[-99.4642629082191,-19.5],[-99.4139302141422,-19.75],[-99.3612634315101,-20],[-99.306249297595,-20.25],[-99.2488738510232,-20.5],[-99.1891224129621,-20.75],[-99.1269795670826,-21],[-99.0624291382309,-21.25],[-98.995454169735,-21.5],[-98.9260368992678,-21.75],[-98.8541587331799,-22],[-98.7798002192098,-22.25],[-98.7029410174709,-22.5],[-98.6235598696041,-22.75],[-98.5416345659799,-23],[-98.4571419108184,-23.25],[-98.3700576850888,-23.5],[-98.2803566070357,-23.75],[-98.188012290165,-24],[-98.0929971985107,-24.25],[-97.9952825989835,-24.5],[-97.8948385105876,-24.75],[-97.7916336502698,-25],[-97.6856353751441,-25.25],[-97.5768096208106,-25.5],[-97.4651208354592,-25.75],[-97.3505319094211,-26],[-97.2330040997937,-26.25],[-97.1124969497314,-26.5],[-96.9889682019496,-26.75],[-96.8623737059448,-27],[-96.7326673183792,-27.25],[-96.5998007960223,-27.5],[-96.463723680573,-27.75],[-96.3243831746128,-28],[-96.1817240078565,-28.25],[-96.0356882927706,-28.5],[-95.8862153685233,-28.75],[-95.7332416321053,-29],[-95.5767003553228,-29.25],[-95.4165214862028,-29.5],[-95.2526314331697,-29.75],[-95.0849528301415,-30],[-94.9134042804544,-30.25],[-94.7379000772445,-30.5],[-94.5583498975967,-30.75],[-94.3746584673957,-31],[-94.1867251933813,-31.25],[-93.994443758404,-31.5],[-93.7977016752848,-31.75],[-93.5963797939844,-32],[-93.3903517559677,-32.25],[-93.1794833886788,-32.5],[-92.9636320318813,-32.75],[-92.742645786248,-33],[-92.516362672927,-33.25],[-92.2846096908265,-33.5],[-92.0472017559569,-33.75],[-91.803940504247,-34],[-91.5546129356814,-34.25],[-91.2989898732233,-34.5],[-91.036824204563,-34.75],[-90.7678488679977,-35],[-90.4917745353087,-35.25],[-90.2082869338697,-35.5],[-89.917043736713,-35.75],[-89.6176709319934,-36],[-89.3097585609688,-36.25],[-89,-36.5],[-89,-12.5]])
    add_zone('D04', [[-89,36.5],[-89,28],[-60,42.5],[-71.5,42.5],[-75.2666297933298,42.25],[-76.8150729063673,42],[-77.9951905283833,41.75],[-78.9833147735479,41.5],[-79.847903928532,41.25],[-80.6241437954473,41],[-81.3329802196486,40.75],[-81.9880884817015,40.5],[-82.5989864402116,40.25],[-83.1726175299288,40],[-83.7142335003061,39.75],[-84.2279220613579,39.5],[-84.7169398878863,39.25],[-85.183932183404,39],[-85.6310827610626,38.75],[-86.060219778561,38.5],[-86.472892172189,38.25],[-86.8704261489394,38],[-87.2539677541882,37.75],[-87.6245154965971,37.5],[-87.9829457318769,37.25],[-88.3300326797068,37],[-88.6664644001029,36.75],[-89,36.5]])
    add_zone('D05', [[-89,28],[-89,16],[-73,24],[-73,36],[-89,28]])
    add_zone('D06', [[-89,16],[-89,4],[-73,12],[-73,24],[-89,16]])
    add_zone('D07', [[-89,4],[-89,-4],[-83,-7],[-83,7],[-89,4]])
    add_zone('D08', [[-89,-4],[-89,-16],[-73,-24],[-73,-12],[-89,-4]])
    add_zone('D09', [[-89,-16],[-89,-28],[-73,-36],[-73,-24],[-89,-16]])
    add_zone('D10', [[-89,-36.5],[-89,-28],[-60,-42.5],[-71.5,-42.5],[-75.2666297933298,-42.25],[-76.8150729063673,-42],[-77.9951905283833,-41.75],[-78.9833147735479,-41.5],[-79.847903928532,-41.25],[-80.6241437954473,-41],[-81.3329802196486,-40.75],[-81.9880884817015,-40.5],[-82.5989864402116,-40.25],[-83.1726175299288,-40],[-83.7142335003061,-39.75],[-84.2279220613579,-39.5],[-84.7169398878863,-39.25],[-85.183932183404,-39],[-85.6310827610626,-38.75],[-86.060219778561,-38.5],[-86.472892172189,-38.25],[-86.8704261489394,-38],[-87.2539677541882,-37.75],[-87.6245154965971,-37.5],[-87.9829457318769,-37.25],[-88.3300326797068,-37],[-88.6664644001029,-36.75],[-89,-36.5]])
    add_zone('D11', [[-83,7],[-83,-7],[-73,-12],[-73,12],[-83,7]])
    add_zone('D12', [[-73,36],[-73,24],[-57,32],[-57,42.5],[-60,42.5],[-73,36]])
    add_zone('D13', [[-73,24],[-73,12],[-57,20],[-57,32],[-73,24]])
    add_zone('D14', [[-73,12],[-73,0],[-57,8],[-57,20],[-73,12]])
    add_zone('D15', [[-73,0],[-57,-8],[-57,8],[-73,0]])
    add_zone('D16', [[-73,0],[-73,-12],[-57,-20],[-57,-8],[-73,0]])
    add_zone('D17', [[-73,-12],[-73,-24],[-57,-32],[-57,-20],[-73,-12]])
    add_zone('D18', [[-73,-24],[-73,-36],[-60,-42.5],[-57,-42.5],[-57,-32],[-73,-24]])
    add_zone('D19', [[-57,42.5],[-57,20],[-41,28],[-41,42.5],[-57,42.5]])
    add_zone('D20', [[-57,20],[-57,8],[-41,16],[-41,28],[-57,20]])
    add_zone('D21', [[-57,8],[-57,-8],[-41,-16],[-41,16],[-57,8]])
    add_zone('D22', [[-57,-8],[-57,-20],[-41,-28],[-41,-16],[-57,-8]])
    add_zone('D23', [[-57,-20],[-57,-42.5],[-41,-42.5],[-41,-28],[-57,-20]])
    add_zone('D24', [[-41,42.5],[-41,16],[-25,24],[-25,42.5],[-41,42.5]])
    add_zone('D25', [[-41,16],[-41,-16],[-25,-24],[-25,24],[-41,16]])
    add_zone('D26', [[-41,-16],[-41,-42.5],[-25,-42.5],[-25,-24],[-41,-16]])

    def point_in_poly(px: float, py: float, poly: List[Tuple[float,float]]) -> bool:
        inside = False
        n = len(poly)
        j = n - 1
        for i in range(n):
            xi, yi = poly[i]
            xj, yj = poly[j]
            intersect = ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi + 1e-9) + xi)
            if intersect:
                inside = not inside
            j = i
        return inside

    # Emit CSV rows
    eid = 1
    for i, ev in enumerate(final_events):
        d = ev.get('details') or {}
        ev_key_norm = normalize_key(ev.get('_overrideEvent') or ev.get('event') or '')  # e.g., 'shot','goal','penalty','blocked_shot'
        event_type = prettify_event_label(ev_key_norm)

        team_id = str(ev.get('_computedTeamId') or '')
        # Attempt to infer/correct team for common events using lineup index and context
        if ev_key_norm in ('shot','goal','block','blocked_shot','so_goal','so_miss'):
            # Prefer shooter/skater; else infer from goalie opponent
            shooter_like = d.get('shooter') or d.get('player') or d.get('scoredBy')
            inferred = ''
            try:
                inferred = get_player_team_from_lineups(shooter_like)
            except Exception:
                inferred = ''
            if not inferred and isinstance(d.get('goalie'), dict):
                try:
                    g_team = get_player_team_from_lineups(d.get('goalie'))
                except Exception:
                    g_team = ''
                if g_team:
                    home_id, away_id = get_team_ids()
                    inferred = home_id if str(g_team) == str(away_id) else away_id if str(g_team) == str(home_id) else ''
            if inferred:
                team_id = str(inferred)
            # If still missing and shooterTeamId present, use it directly
            if not team_id and d.get('shooterTeamId') is not None:
                team_id = str(d.get('shooterTeamId'))
        elif ev_key_norm == 'penalty':
            # Infer penalized team from takenBy/servedBy; or as opponent of drawnBy; else fallbacks
            inferred = ''
            taker_like = d.get('takenBy') or d.get('player') or d.get('servedBy')
            if taker_like:
                try:
                    inferred = get_player_team_from_lineups(taker_like)
                except Exception:
                    inferred = ''
            if not inferred and d.get('drawnBy'):
                try:
                    draw_team = get_player_team_from_lineups(d.get('drawnBy'))
                except Exception:
                    draw_team = ''
                if draw_team:
                    home_id, away_id = get_team_ids()
                    inferred = home_id if str(draw_team) == str(away_id) else away_id if str(draw_team) == str(home_id) else ''
            if not inferred:
                inferred = str((d.get('team') or {}).get('id') or d.get('teamId') or d.get('team_id') or '')
            if inferred:
                team_id = str(inferred)

        # Derive team name and venue using event-type specific primary sources
        resolved = False
        team_name = ''
        home_name = str(game.get('home_team') or '')
        away_name = str(game.get('away_team') or '')
        etl = ev_key_norm
        # Preferred numeric team id per event type
        pref_tid = ''
        if etl == 'goal':
            # Prefer merged computed team id (scoring team) over raw details.team which may be ambiguous
            pref_tid = str(ev.get('_computedTeamId') or (d.get('team') or {}).get('id') or '')
        elif etl in ('shot','so_goal','so_miss'):
            pref_tid = str(d.get('shooterTeamId') or '')
        elif etl == 'penalty':
            pref_tid = str((d.get('againstTeam') or {}).get('id') or '')
        elif etl == 'hit':
            pref_tid = str(d.get('teamId') or '')
        elif etl in ('block','blocked_shot'):
            # Adjusted per new requirement: record Block for the SHOOTING team, not the blocker.
            # We attempt to find the original shooting team by using againstTeam (the team whose attempt was blocked)
            # or by inferring from goalie context; fallback to previous blocker logic only if shooter side cannot be resolved.
            shooter_team = ''
            # Many feeds store the blocking team in details.team and the shooting team in againstTeam.
            at = d.get('againstTeam') or {}
            if isinstance(at, dict):
                shooter_team = str(at.get('id') or '')
            if not shooter_team:
                # Try shooterTeamId if present
                shooter_team = str(d.get('shooterTeamId') or '')
            if not shooter_team:
                # Infer via goalie (goalie belongs to defending team of the shot; so shooting team is opponent)
                try:
                    g_team = get_player_team_from_lineups(d.get('goalie')) if d.get('goalie') else ''
                except Exception:
                    g_team = ''
                if g_team:
                    home_id, away_id = get_team_ids()
                    shooter_team = home_id if str(g_team) == str(away_id) else away_id if str(g_team) == str(home_id) else ''
            if shooter_team:
                pref_tid = str(shooter_team)
            else:
                # Fallback (rare) – revert to blocker team so event still has a team tag.
                bt = ''
                try:
                    bt = get_player_team_from_lineups(d.get('player') or d.get('blocker'))
                except Exception:
                    bt = ''
                if bt:
                    pref_tid = str(bt)
        # Fall back to whatever was computed earlier if still empty
        if not pref_tid and team_id:
            pref_tid = str(team_id)
        # Map preferred id to team name using schedule ids or id->abbr tables
        is_home = None  # None means unknown until resolved below
        if pref_tid and not resolved:
            # If the preferred id matches schedule ids, we can resolve directly
            home_id, away_id = get_team_ids()
            if pref_tid == str(home_id):
                team_name = home_name
                is_home = True
                resolved = True
            elif pref_tid == str(away_id):
                team_name = away_name
                is_home = False
                resolved = True
            elif pref_tid in id_to_abbr:
                ab = id_to_abbr[pref_tid]
                nm = code_to_name.get(ab) or ''
                if nm:
                    team_name = nm
                    # Attempt to deduce venue by comparing to schedule names
                    if nm and home_name and nm.lower() == home_name.lower():
                        is_home = True
                    elif nm and away_name and nm.lower() == away_name.lower():
                        is_home = False
                    resolved = True
        # As a last mapping attempt, use inline abbreviation fields on this event
        if not resolved:
            ab = ''
            tdict = d.get('team') or {}
            atdict = d.get('againstTeam') or {}
            if isinstance(tdict, dict):
                ab = str(tdict.get('abbreviation') or '').strip()
            if not ab and isinstance(atdict, dict):
                ab = str(atdict.get('abbreviation') or '').strip()
            if ab:
                nm = code_to_name.get(ab) or ''
                if nm:
                    team_name = nm
                    if nm and home_name and nm.lower() == home_name.lower():
                        is_home = True
                    elif nm and away_name and nm.lower() == away_name.lower():
                        is_home = False
                    resolved = True
        if not team_name:
            team_name = (d.get('team') or {}).get('name') or (d.get('team') or {}).get('fullName') or ''
        # Determine venue; prefer id, then name, then on-ice composition
        if not resolved:
            home_id, away_id = get_team_ids()
            if team_id and str(game.get('home_team_id') or ''):
                is_home = (team_id == str(game.get('home_team_id') or ''))
                resolved = True
            else:
                ht = str(game.get('home_team') or '').lower()
                at = str(game.get('away_team') or '').lower()
                tn = str(team_name or '').lower()
                if tn == ht:
                    is_home = True
                    resolved = True
                elif tn == at:
                    is_home = False
                    resolved = True
                else:
                    # Fuzzy: accent-folded substring matching on city/team tokens
                    try:
                        import unicodedata
                        def fold(s: str) -> str:
                            s2 = unicodedata.normalize('NFD', s)
                            return ''.join(ch for ch in s2 if unicodedata.category(ch) != 'Mn').lower()
                        nf_ht = fold(str(game.get('home_team') or ''))
                        nf_at = fold(str(game.get('away_team') or ''))
                        nf_tn = fold(team_name)
                        # Use first token (likely city)
                        ht_tok = nf_ht.split(' ')[0] if nf_ht else ''
                        at_tok = nf_at.split(' ')[0] if nf_at else ''
                        if ht_tok and ht_tok in nf_tn:
                            is_home = True
                            resolved = True
                        elif at_tok and at_tok in nf_tn:
                            is_home = False
                            resolved = True
                    except Exception:
                        pass
        venue = ('Home' if is_home else 'Away') if resolved and is_home is not None else ''
        # Snap team_name to schedule naming only when resolved
        if resolved and is_home is not None:
            team_name = str(game.get('home_team') if is_home else game.get('away_team') or team_name)

        # players
        p1_no = p1_name = p2_no = p2_name = p3_no = p3_name = g_no = goalie_name = ''
        if ev_key_norm in ('goal','so_goal'):
            scorer = ev.get('_mergedScorer') or d.get('scoredBy') or d.get('scorer') or d.get('player')
            if isinstance(scorer, dict):
                p1_no = str(scorer.get('jerseyNumber') or scorer.get('id') or '')
                p1_name = (str(scorer.get('firstName') or '') + ' ' + str(scorer.get('lastName') or '')).strip()
            assists = ev.get('_mergedAssists') if isinstance(ev.get('_mergedAssists'), list) else (d.get('assists') or [])
            if isinstance(assists, list):
                if len(assists) > 0 and isinstance(assists[0], dict):
                    p2_no = str(assists[0].get('jerseyNumber') or assists[0].get('id') or '')
                    p2_name = (str(assists[0].get('firstName') or '') + ' ' + str(assists[0].get('lastName') or '')).strip()
                if len(assists) > 1 and isinstance(assists[1], dict):
                    p3_no = str(assists[1].get('jerseyNumber') or assists[1].get('id') or '')
                    p3_name = (str(assists[1].get('firstName') or '') + ' ' + str(assists[1].get('lastName') or '')).strip()
            gsrc = ev.get('_mergedGoalie') or d.get('goalie')
            if isinstance(gsrc, dict):
                g_no = str(gsrc.get('jerseyNumber') or gsrc.get('id') or '')
                goalie_name = (str(gsrc.get('firstName') or '') + ' ' + str(gsrc.get('lastName') or '')).strip()
        elif ev_key_norm == 'penalty':
            taker = d.get('takenBy') or d.get('player') or d.get('servedBy')
            if isinstance(taker, dict):
                p1_no = str(taker.get('jerseyNumber') or taker.get('id') or '')
                p1_name = (str(taker.get('firstName') or '') + ' ' + str(taker.get('lastName') or '')).strip()
            drawer = d.get('drawnBy')
            if isinstance(drawer, dict):
                p2_no = str(drawer.get('jerseyNumber') or drawer.get('id') or '')
                p2_name = (str(drawer.get('firstName') or '') + ' ' + str(drawer.get('lastName') or '')).strip()
        else:
            player = d.get('shooter') or d.get('player')
            if isinstance(player, dict):
                p1_no = str(player.get('jerseyNumber') or player.get('id') or '')
                p1_name = (str(player.get('firstName') or '') + ' ' + str(player.get('lastName') or '')).strip()
            if isinstance(d.get('goalie'), dict):
                g_no = str(d['goalie'].get('jerseyNumber') or d['goalie'].get('id') or '')
                goalie_name = (str(d['goalie'].get('firstName') or '') + ' ' + str(d['goalie'].get('lastName') or '')).strip()

        # On-ice players: choose plus/minus arrays; then split by venue
        def to_list(arr: Any) -> List[str]:
            if not isinstance(arr, list):
                return []
            out: List[str] = []
            for p in arr:
                if isinstance(p, dict):
                    out.append(str(p.get('jerseyNumber') or p.get('id') or ''))
                else:
                    out.append(str(p or ''))
            return [x for x in out if x]
        def to_names(arr: Any) -> List[str]:
            if not isinstance(arr, list):
                return []
            out: List[str] = []
            for p in arr:
                if isinstance(p, dict):
                    nm = (str(p.get('firstName') or '') + ' ' + str(p.get('lastName') or '')).strip()
                    if nm:
                        out.append(nm)
                else:
                    if str(p or '').strip():
                        out.append(str(p))
            return out
        plus_players = d.get('plus_players') or d.get('plusPlayers') or d.get('homePlayers') or d.get('homeOnIce') or []
        minus_players = d.get('minus_players') or d.get('minusPlayers') or d.get('awayPlayers') or d.get('awayOnIce') or []
        home_players = plus_players if is_home else minus_players
        away_players = minus_players if is_home else plus_players
        home_players_no = ' '.join(to_list(home_players))
        home_players_names = ' - '.join(to_names(home_players))
        away_players_no = ' '.join(to_list(away_players))
        away_players_names = ' - '.join(to_names(away_players))

        # period value
        period = ''
        if ev_key_norm in ('so_goal','so_miss'):
            period = 'SO'
        else:
            p = d.get('period')
            if isinstance(p, dict):
                period = str(p.get('shortName') or p.get('id') or '')
            elif p is not None:
                period = str(p)

        # coordinates (raw)
        x_raw = ev.get('_x') if ev.get('_x') is not None else (d.get('xLocation') if d.get('xLocation') is not None else d.get('xCoord'))
        y_raw = ev.get('_y') if ev.get('_y') is not None else (d.get('yLocation') if d.get('yLocation') is not None else d.get('yCoord'))
        # Orient so shooting team is always in offensive end (positive x after convert)
        # Assume raw rink dims: x in [0,600], y in [0,300]; mirror for home events.
        # If dimensions unknown, fall back to simple sign inversion after normalization.
        x_or = x_raw
        y_or = y_raw
        try:
            xr = float(x_raw) if x_raw is not None and x_raw != '' else None
            yr = float(y_raw) if y_raw is not None and y_raw != '' else None
            if xr is not None and yr is not None and is_home is True:
                # Mirror across center to make home offensive to the right (+x)
                x_or = 600 - xr
                y_or = 300 - yr
        except Exception:
            # Fallback: keep as-is
            x_or = x_raw
            y_or = y_raw
        x_norm = convert_x(x_or)
        y_norm = convert_y(y_or)

        # Compute BoxID using oriented normalized coords
        box_id = ''
        try:
            px = float(x_norm) if x_norm not in ('', None) else None
            py = float(y_norm) if y_norm not in ('', None) else None
            if px is not None and py is not None:
                for zid, poly in ZONES:
                    if point_in_poly(px, py, poly):
                        box_id = zid
                        break
        except Exception:
            box_id = ''

        # ScoreState: running (team goals - opp goals) computed before applying goal increment
        score_state = ''
        if is_home is not None:
            if ev_key_norm == 'goal':
                # compute before increment, no additional subtraction
                diff = (home_goals - away_goals) if is_home else (away_goals - home_goals)
                score_state = str(diff)
                # now apply increment
                if is_home:
                    home_goals += 1
                else:
                    away_goals += 1
            else:
                diff = (home_goals - away_goals) if is_home else (away_goals - home_goals)
                score_state = str(diff)
        else:
            score_state = ''

        # Compute xG for Shots and Goals only
        xg_val = ''
        if ev_key_norm in ('shot','goal'):
            xg_val = xg_for(strengths[i] or '', score_state, box_id)

        row = [
            str(eid),
            str(d.get('time') or ''),
            event_type,
            team_name,
            venue,
            str(game.get('home_team') or ''),
            str(game.get('away_team') or ''),
            period,
            'event',
            strengths[i] or '',
            p1_no, p1_name, p2_no, p2_name, p3_no, p3_name,
            g_no, goalie_name,
            '', home_players_no, home_players_names,
            '', away_players_no, away_players_names,
            x_norm, y_norm, xg_val,
            score_state, box_id,
            str(game_id or ''),
            game_date,
            'PWHL',
            str(game.get('season_year') or ''),
            str(game.get('season_state') or '')
        ]
        writer.writerow(row)
        eid += 1

    return out.getvalue()


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
