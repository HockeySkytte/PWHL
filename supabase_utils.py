"""Supabase helpers for PWHL Analytics.

Provides upsert functions for teams, lineups, and play-by-play data.
CSV strings produced by export_utils are parsed into dicts and batch-upserted.
"""
from __future__ import annotations

import csv
import io
import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Load .env at import time so SUPABASE_URL/KEY are available
load_dotenv()

# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------

_client = None


def get_supabase_client():
    """Return a cached Supabase client (creates on first call)."""
    global _client
    if _client is not None:
        return _client

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key or "your-" in url or "your-" in key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the environment "
            "(or in a .env file).  See .env.example."
        )

    from supabase import create_client
    _client = create_client(url, key)
    return _client


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _csv_string_to_dicts(csv_text: str) -> List[Dict[str, str]]:
    """Parse a CSV string (with header row) into a list of dicts."""
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def _batch_upsert(
    table: str,
    rows: List[Dict[str, Any]],
    on_conflict: str,
    batch_size: int = 500,
) -> int:
    """Upsert *rows* into *table* in batches.  Returns total row count."""
    if not rows:
        return 0
    client = get_supabase_client()
    total = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
        total += len(chunk)
    return total


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

def upsert_teams(teams_csv_path: str) -> int:
    """Read Teams.csv and upsert all rows into pwhl_teams.

    Returns the number of rows upserted.
    """
    if not os.path.exists(teams_csv_path):
        raise FileNotFoundError(f"Teams CSV not found: {teams_csv_path}")

    with open(teams_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, Any]] = []
        for r in reader:
            tid = (r.get("id") or r.get("\ufeffid") or "").strip()
            if not tid:
                continue
            rows.append({
                "id": int(tid),
                "name": (r.get("name") or "").strip(),
                "nickname": (r.get("nickname") or "").strip(),
                "team_code": (r.get("team_code") or r.get("code") or "").strip(),
                "logo": (r.get("logo") or "").strip(),
                "color": (r.get("color") or "").strip(),
            })

    return _batch_upsert("pwhl_teams", rows, on_conflict="id")


# ---------------------------------------------------------------------------
# Lineups
# ---------------------------------------------------------------------------

def _safe_int(v: Any) -> Optional[int]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_date(v: Any) -> Optional[str]:
    s = str(v or "").strip()
    return s if s else None


def upsert_lineups_from_csv(csv_text: str) -> int:
    """Parse a lineups CSV string and upsert into pwhl_lineups.

    The CSV is produced by export_utils.generate_lineups_csv().
    Returns the number of rows upserted.
    """
    raw = _csv_string_to_dicts(csv_text)
    if not raw:
        return 0

    rows: List[Dict[str, Any]] = []
    for r in raw:
        rows.append({
            "game_id": _safe_int(r.get("Game ID")),
            "game_date": _safe_date(r.get("Date")),
            "team": r.get("Team", ""),
            "team_color": r.get("Team Color", ""),
            "venue": r.get("Venue", ""),
            "number": _safe_int(r.get("Number")),
            "name": r.get("Name", ""),
            "position": r.get("Line", ""),          # CSV column "Line" = position
            "toi": _safe_int(r.get("TOI")),
            "competition": r.get("Competition", "PWHL"),
            "season": r.get("Season", ""),
            "state": r.get("State", ""),
        })

    return _batch_upsert("pwhl_lineups", rows, on_conflict="game_id,name,venue")


# ---------------------------------------------------------------------------
# Play-by-play
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> Optional[float]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def upsert_pbp_from_csv(csv_text: str) -> int:
    """Parse a PBP CSV string and upsert into pwhl_pbp.

    The CSV is produced by export_utils.generate_pbp_csv().
    Returns the number of rows upserted.
    """
    raw = _csv_string_to_dicts(csv_text)
    if not raw:
        return 0

    rows: List[Dict[str, Any]] = []
    for r in raw:
        rows.append({
            "game_id": _safe_int(r.get("game_id")),
            "event_id": _safe_int(r.get("id")),
            "game_date": _safe_date(r.get("game_date")),
            "timestamp": r.get("timestamp", ""),
            "event": r.get("event", ""),
            "team": r.get("team", ""),
            "venue": r.get("venue", ""),
            "team_home": r.get("team_home", ""),
            "team_away": r.get("team_away", ""),
            "period": r.get("period", ""),
            "perspective": r.get("perspective", ""),
            "strength": r.get("strength", ""),
            "p1_no": r.get("p1_no", ""),
            "p1_name": r.get("p1_name", ""),
            "p2_no": r.get("p2_no", ""),
            "p2_name": r.get("p2_name", ""),
            "p3_no": r.get("p3_no", ""),
            "p3_name": r.get("p3_name", ""),
            "g_no": r.get("g_no", ""),
            "goalie_name": r.get("goalie_name", ""),
            "home_players": r.get("home_players", ""),
            "home_players_names": r.get("home_players_names", ""),
            "away_players": r.get("away_players", ""),
            "away_players_names": r.get("away_players_names", ""),
            "x": _safe_float(r.get("x")),
            "y": _safe_float(r.get("y")),
            "xg": _safe_float(r.get("xG")),
            "score_state": r.get("ScoreState", ""),
            "box_id": r.get("BoxID", ""),
            "competition": r.get("competition", "PWHL"),
            "season": r.get("season", ""),
            "state": r.get("state", ""),
        })

    return _batch_upsert("pwhl_pbp", rows, on_conflict="game_id,event_id")


# ---------------------------------------------------------------------------
# Convenience: upsert everything for one game
# ---------------------------------------------------------------------------

def upsert_game(
    lineups_csv: str | None = None,
    pbp_csv: str | None = None,
) -> Dict[str, int]:
    """Upsert lineups + PBP for a single game. Returns counts."""
    counts: Dict[str, int] = {}
    if lineups_csv and lineups_csv.strip():
        counts["lineups"] = upsert_lineups_from_csv(lineups_csv)
    if pbp_csv and pbp_csv.strip():
        counts["pbp"] = upsert_pbp_from_csv(pbp_csv)
    return counts


# ===========================================================================
# READ helpers – used by flask_app.py and report_data.py
# ===========================================================================

def fetch_teams() -> List[Dict[str, Any]]:
    """Return all rows from pwhl_teams as dicts."""
    client = get_supabase_client()
    resp = client.table("pwhl_teams").select("*").execute()
    return resp.data or []


def fetch_lineups_for_game(game_id: int) -> List[Dict[str, Any]]:
    """Return all lineup rows for a given game_id."""
    client = get_supabase_client()
    resp = (
        client.table("pwhl_lineups")
        .select("*")
        .eq("game_id", game_id)
        .execute()
    )
    return resp.data or []


def fetch_all_skater_names() -> List[str]:
    """Return sorted unique skater names (position != 'G') from pwhl_lineups."""
    client = get_supabase_client()
    # Fetch only the name column for non-goalie players
    resp = (
        client.table("pwhl_lineups")
        .select("name")
        .neq("position", "G")
        .execute()
    )
    names = sorted({(r.get("name") or "").strip() for r in (resp.data or []) if (r.get("name") or "").strip()})
    return names


def fetch_all_goalie_names() -> List[str]:
    """Return sorted unique goalie names (position == 'G') from pwhl_lineups."""
    client = get_supabase_client()
    resp = (
        client.table("pwhl_lineups")
        .select("name")
        .eq("position", "G")
        .execute()
    )
    names = sorted({(r.get("name") or "").strip() for r in (resp.data or []) if (r.get("name") or "").strip()})
    return names


def fetch_player_team(player_name: str, game_id: Optional[int] = None) -> str:
    """Return the team name for a player, optionally scoped to a specific game.

    If game_id is provided, looks up that game only.
    Otherwise returns the team from the most recent game (by game_date desc).
    """
    client = get_supabase_client()
    q = (
        client.table("pwhl_lineups")
        .select("team, game_id, game_date")
        .eq("name", player_name)
    )
    if game_id is not None:
        q = q.eq("game_id", game_id)
    resp = q.order("game_date", desc=True).limit(1).execute()
    rows = resp.data or []
    if rows:
        return (rows[0].get("team") or "").strip()
    return ""


def fetch_lineup_detail_for_game(game_id: int) -> List[Dict[str, Any]]:
    """Return full lineup rows (name, team, venue, position, toi) for a game."""
    client = get_supabase_client()
    resp = (
        client.table("pwhl_lineups")
        .select("name, team, venue, position, toi, game_date, season, state, number")
        .eq("game_id", game_id)
        .execute()
    )
    return resp.data or []
