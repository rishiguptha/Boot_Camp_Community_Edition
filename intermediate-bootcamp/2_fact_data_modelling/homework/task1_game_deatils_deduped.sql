-- Task 1: Game details at game/team/player grain — dedupe on load for idempotent facts; dim_/m_ separates filter dims from additive measures.
WITH
    deduped AS (
        -- Reapply ROW_NUMBER dedupe so replays do not double-count the same lineup row.
        SELECT g.game_date_est, g.season, g.home_team_id, gd.*, ROW_NUMBER() OVER (
                PARTITION BY
                    gd.game_id, team_id, player_id
                ORDER BY g.game_date_est
            ) AS row_num
        FROM game_details as gd
            JOIN games g ON gd.game_id = g.game_id
    )
SELECT
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
    SPLIT_PART(min, ':', 1)::REAL + SPLIT_PART(min, ':', 2)::REAL / 60 AS m_minutes, -- min stored as "MM:SS"; convert to decimal minutes
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