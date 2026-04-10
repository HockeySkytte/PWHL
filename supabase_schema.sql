-- =============================================================================
-- PWHL Analytics – Supabase Table Definitions
-- Run this in the Supabase SQL Editor to create all tables.
-- =============================================================================

-- ─── pwhl_teams ─────────────────────────────────────────────────────────────
-- Source of truth for team identity (mirrors Teams.csv).
-- Upserted once from Teams.csv; rarely changes.
CREATE TABLE IF NOT EXISTS pwhl_teams (
    id          INT PRIMARY KEY,            -- HockeyTech team id (1-9)
    name        TEXT NOT NULL,              -- "Boston Fleet"
    nickname    TEXT,                        -- "Fleet"
    team_code   TEXT,                        -- "BOS"
    logo        TEXT,                        -- URL to logo PNG
    color       TEXT,                        -- hex color "#004D29"
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- ─── pwhl_players ───────────────────────────────────────────────────────────
-- Unique player registry extracted from lineups across all games.
-- Natural key: (player_id).  jersey_number and team can change per game/season.
CREATE TABLE IF NOT EXISTS pwhl_players (
    player_id       INT PRIMARY KEY,        -- HockeyTech player id
    first_name      TEXT,
    last_name       TEXT,
    name            TEXT NOT NULL,           -- "Kendall Coyne Schofield"
    jersey_number   INT,                    -- most-recent jersey
    position        TEXT,                   -- G / LD / RD / C / LW / RW
    team_id         INT REFERENCES pwhl_teams(id),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ─── pwhl_lineups ───────────────────────────────────────────────────────────
-- One row per player per game (roster + TOI).
-- Composite PK: (game_id, player_name, venue) to handle edge cases.
CREATE TABLE IF NOT EXISTS pwhl_lineups (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    game_id         INT NOT NULL,
    game_date       DATE,
    team            TEXT NOT NULL,           -- "Minnesota Frost"
    team_color      TEXT,                   -- "#353364"
    venue           TEXT NOT NULL,           -- "Home" / "Away"
    number          INT,                    -- jersey number
    name            TEXT NOT NULL,           -- "Maddie Rooney"
    position        TEXT,                   -- "G", "LD", "C", …
    toi             INT,                    -- time on ice in seconds
    competition     TEXT DEFAULT 'PWHL',
    season          TEXT,                   -- "2025/2026"
    state           TEXT,                   -- "Regular Season"
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (game_id, name, venue)
);

CREATE INDEX IF NOT EXISTS idx_lineups_game ON pwhl_lineups (game_id);
CREATE INDEX IF NOT EXISTS idx_lineups_team ON pwhl_lineups (team);
CREATE INDEX IF NOT EXISTS idx_lineups_season ON pwhl_lineups (season);

-- ─── pwhl_pbp ───────────────────────────────────────────────────────────────
-- Play-by-play events (shots, goals, blocks, penalties, etc.).
-- One row per event per game.
-- Composite unique: (game_id, event_id) where event_id is the sequential id within the game.
CREATE TABLE IF NOT EXISTS pwhl_pbp (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    game_id         INT NOT NULL,
    event_id        INT NOT NULL,           -- sequential id within game (from CSV 'id' column)
    game_date       DATE,
    timestamp       TEXT,                   -- "4:00" (period clock)
    event           TEXT NOT NULL,           -- "Shot", "Goal", "Block", "Penalty", …
    team            TEXT,                   -- "Minnesota Frost"
    venue           TEXT,                   -- "Home" / "Away"
    team_home       TEXT,
    team_away       TEXT,
    period          TEXT,                   -- "1", "2", "3", "OT", "SO"
    perspective     TEXT,                   -- "event"
    strength        TEXT,                   -- "5v5", "5v4", "ENA", …
    p1_no           TEXT,
    p1_name         TEXT,                   -- scorer / shooter / penalized player
    p2_no           TEXT,
    p2_name         TEXT,                   -- 1st assist / drawn-by
    p3_no           TEXT,
    p3_name         TEXT,                   -- 2nd assist
    g_no            TEXT,
    goalie_name     TEXT,
    home_players    TEXT,                   -- space-separated jersey numbers
    home_players_names TEXT,               -- pipe-separated names
    away_players    TEXT,
    away_players_names TEXT,
    x               REAL,                   -- normalized x coordinate (feet)
    y               REAL,                   -- normalized y coordinate (feet)
    xg              REAL,                   -- expected goals value
    score_state     TEXT,                   -- "-1", "0", "1", …
    box_id          TEXT,                   -- "O07", "N_or_D", …
    competition     TEXT DEFAULT 'PWHL',
    season          TEXT,                   -- "2025/2026"
    state           TEXT,                   -- "Regular Season"
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (game_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_pbp_game ON pwhl_pbp (game_id);
CREATE INDEX IF NOT EXISTS idx_pbp_event ON pwhl_pbp (event);
CREATE INDEX IF NOT EXISTS idx_pbp_team ON pwhl_pbp (team);
CREATE INDEX IF NOT EXISTS idx_pbp_season ON pwhl_pbp (season);
CREATE INDEX IF NOT EXISTS idx_pbp_game_date ON pwhl_pbp (game_date);

-- ─── Row-Level Security (RLS) ──────────────────────────────────────────────
-- Enable RLS but allow public reads (anon key) on all tables.
-- Writes are done via service_role key from the export script.

ALTER TABLE pwhl_teams   ENABLE ROW LEVEL SECURITY;
ALTER TABLE pwhl_players ENABLE ROW LEVEL SECURITY;
ALTER TABLE pwhl_lineups ENABLE ROW LEVEL SECURITY;
ALTER TABLE pwhl_pbp     ENABLE ROW LEVEL SECURITY;

-- Public read policies
CREATE POLICY "Public read pwhl_teams"   ON pwhl_teams   FOR SELECT USING (true);
CREATE POLICY "Public read pwhl_players" ON pwhl_players FOR SELECT USING (true);
CREATE POLICY "Public read pwhl_lineups" ON pwhl_lineups FOR SELECT USING (true);
CREATE POLICY "Public read pwhl_pbp"     ON pwhl_pbp     FOR SELECT USING (true);

-- Service-role write policies (service_role bypasses RLS by default,
-- but explicit policies make intent clear)
CREATE POLICY "Service write pwhl_teams"   ON pwhl_teams   FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service write pwhl_players" ON pwhl_players FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service write pwhl_lineups" ON pwhl_lineups FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service write pwhl_pbp"     ON pwhl_pbp     FOR ALL USING (true) WITH CHECK (true);
