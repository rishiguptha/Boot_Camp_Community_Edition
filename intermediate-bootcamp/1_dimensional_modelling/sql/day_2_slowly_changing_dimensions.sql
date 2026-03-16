-- slowly changing dimensions
DROP TABLE players_scd;
CREATE TABLE players_scd(
    player_name VARCHAR,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY(player_name, start_season)
);

-- This query creates an SCD Type 2 history for players up to 2021.
-- It detects season-to-season changes in scoring_class or is_active,
-- groups continuous runs of the same state, and writes each run as
-- one record with start_season and end_season in players_scd.

INSERT INTO players_scd -- Step 1: Build a season-by-season history per player and attach prior-season values
    -- so each row can be compared against the immediately previous state.
    WITH with_previous AS (
        SELECT player_name,
            current_season,
            scoring_class,
            is_active,
            LAG(scoring_class, 1) OVER (
                PARTITION BY player_name
                ORDER BY current_season
            ) AS previous_scoring_class,
            LAG(is_active, 1) OVER (
                PARTITION BY player_name
                ORDER BY current_season
            ) AS previous_is_active
        FROM players -- Restrict to the historical window that will be materialized as the 2021 snapshot.
        WHERE current_season <= 2021
    ),
    -- Step 2: Create a change flag. A row is marked as a boundary (1) when either
    -- scoring_class or is_active differs from the previous season; otherwise it is 0.
    with_indicators AS (
        SELECT *,
            CASE
                WHEN scoring_class <> previous_scoring_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END AS change_indicator
        FROM with_previous
    ),
    -- Step 3: Convert change boundaries into contiguous groups using a running SUM.
    -- Each uninterrupted period of identical state gets one streak_identifier.
    with_steaks AS (
        SELECT *,
            SUM(change_indicator) OVER (
                PARTITION BY player_name
                ORDER BY current_season
            ) as streak_identifier
        FROM with_indicators
    ) 

-- Step 4: Aggregate each streak into a single SCD record with effective dates:
-- MIN(current_season) is start_season, MAX(current_season) is end_season.
-- current_season is set to 2021 to label this output as the 2021 SCD snapshot.
SELECT player_name,
    scoring_class,
    is_active,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season,
    2021 AS current_season
FROM with_steaks
GROUP BY player_name,
    streak_identifier,
    scoring_class,
    is_active
ORDER BY player_name,
    streak_identifier;
SELECT *
FROM players_scd;



-- Purpose:
-- This query performs an incremental SCD Type 2 update from 2021 -> 2022.
-- It takes the 2021 SCD snapshot and 2022 player data, then outputs the new
-- full SCD state by combining:
-- 1) historical closed records,
-- 2) unchanged current records (extend end_season),
-- 3) changed records (close old + open new),
-- 4) brand-new players in 2022.

-- Helper composite type used to emit both "old" and "new" SCD rows
-- for changed players in one array and then unnest them.
CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);

WITH
-- Active SCD rows from the 2021 snapshot (the ones eligible to be updated in 2022).
last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
      AND end_season = 2021
),

-- Older historical rows already closed before 2021; carry forward unchanged.
historical_scd AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),

-- Source-of-truth attributes for the new incoming season (2022).
this_season_data AS (
    SELECT *
    FROM players
    WHERE current_season = 2022
),

-- Players whose state did not change from 2021 to 2022:
-- keep same start_season and extend end_season to 2022.
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
      ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

-- Players whose state changed:
-- produce two records per player:
--   1) old version (already closed at previous end_season),
--   2) new version (opens in 2022, ends in 2022 for now).
changed_records AS (
    SELECT
        ts.player_name,
        UNNEST(
            ARRAY[
                ROW(
                    ls.scoring_class,
                    ls.is_active,
                    ls.start_season,
                    ls.end_season
                )::scd_type,
                ROW(
                    ts.scoring_class,
                    ts.is_active,
                    ts.current_season,
                    ts.current_season
                )::scd_type
            ]
        ) AS records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
      ON ls.player_name = ts.player_name
    WHERE ts.scoring_class <> ls.scoring_class
       OR ts.is_active <> ls.is_active
),

-- Expand composite records back into standard columns.
unnested_changed_records AS (
    SELECT
        player_name,
        (records::scd_type).*
    FROM changed_records
),

-- Players appearing in 2022 with no prior SCD row:
-- create their first SCD record starting and ending in 2022.
new_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
      ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

-- Final incremental SCD output for the 2022 snapshot.
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;