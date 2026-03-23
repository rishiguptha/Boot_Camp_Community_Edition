-- Validate table grain assumptions: each (game, team, player) should be unique.
SELECT game_id, team_id, player_id, COUNT(1)
FROM game_details
GROUP BY
    1,
    2,
    3
HAVING
    COUNT(1) > 1;

WITH
    deduped AS (
        -- Rank potential duplicates so we can deterministically keep one row per grain.
        SELECT g.game_date_est, g.season, g.home_team_id, gd.*, ROW_NUMBER() OVER (
                PARTITION BY
                    gd.game_id, team_id, player_id
                ORDER BY g.game_date_est
            ) AS row_num
        FROM game_details as gd
            JOIN games g ON gd.game_id = g.game_id
        WHERE
            g.game_date_est = '2016-10-04'
    )
    -- Build a cleaned single-day player-game view with derived participation flags and core box-score stats.
SELECT
    game_date_est,
    season,
    team_id,
    player_id,
    player_name,
    start_position,
    team_id = home_team_id AS dim_is_playing_at_home, -- boolean dims make common filters cheap and readable.
    COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
    SPLIT_PART(min, ':', 1)::REAL + SPLIT_PART(min, ':', 2)::REAL / 60 AS minutes,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" AS turnovers,
    pf,
    pts,
    plus_minus
FROM deduped
WHERE
    -- Keep only one record per (game, team, player) grain.
    row_num = 1;

-- Fact table keeps high-value analytical fields; low-value descriptive attributes stay in dimensions.
CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plaus_minus INTEGER,
    PRIMARY KEY (
        -- Grain is one player's stat line for one team on one game date.
        dim_game_date,
        dim_team_id,
        dim_player_id
    )
);

-- Load the fact table from a deduplicated source projection.
INSERT INTO
    fct_game_details
WITH
    deduped AS (
        -- Reapply dedup at load time so fact table remains idempotent and clean.
        SELECT g.game_date_est, g.season, g.home_team_id, gd.*, ROW_NUMBER() OVER (
                PARTITION BY
                    gd.game_id, team_id, player_id
                ORDER BY g.game_date_est
            ) AS row_num
        FROM game_details as gd
            JOIN games g ON gd.game_id = g.game_id
    )
SELECT
    -- Naming convention: dim_* for attributes used in grouping/filtering, m_* for aggregatable measures.
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
    SPLIT_PART(min, ':', 1)::REAL + SPLIT_PART(min, ':', 2)::REAL / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE
    row_num = 1;

-- Quick sanity read of the curated fact.
SELECT * FROM fct_game_details;

-- Demonstrate late enrichment: join team descriptors only at query time.
SELECT t.*, gd.*
FROM fct_game_details gd
    JOIN teams t
    -- Keep team descriptors out of the fact and join only when needed.
    ON t.team_id = gd.dim_team_id;

-- Example analyst query: summarize scoring and roster-status behavior by player and home/away context.
SELECT
    dim_player_name,
    dim_is_playing_at_home,
    COUNT(1) AS num_games,
    SUM(m_pts) AS total_points,
    COUNT(
        CASE
            WHEN dim_not_with_team THEN 1
        END
    ) AS bailed_num,
    -- Ratio metric built from boolean dim for fast behavioral analysis.
    COUNT(
        CASE
            WHEN dim_not_with_team THEN 1
        END
    )::REAL / COUNT(1) AS bailed_pct
FROM fct_game_details
GROUP BY
    1,
    2
ORDER BY 6 DESC;