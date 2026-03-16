SELECT * FROM player_seasons
LIMIT 10;


CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);


DROP TABLE players;
CREATE TABLE players(
    player_name VARCHAR,
    height VARCHAR,
    college VARCHAR,
    country VARCHAR,
    draft_year VARCHAR,
    draft_round VARCHAR,
    draft_number VARCHAR,
    season_stats season_stats[],
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);


-- Full outer join for cummulative table
--Seed using 1995 as yesterday

INSERT INTO players
    WITH yesterday AS (
        SELECT * FROM players
        WHERE current_season = 2000
    ),

    today AS (
        SELECT * FROM player_seasons
        WHERE season = 2001
    )
    SELECT 
        COALESCE(t.player_name, y.player_name) AS player_name,
        COALESCE(t.height, y.height) AS height,
        COALESCE(t.college, y.college) AS college,
        COALESCE(t.country, y.country) AS country,
        COALESCE(t.draft_year, y.draft_year) AS draft_year,
        COALESCE(t.draft_round, y.draft_round) AS draft_round,
        COALESCE(t.draft_number, y.draft_number) AS draft_number,
        CASE WHEN y.season_stats IS NULL 
            THEN ARRAY[
                ROW(t.season, t.gp, t.pts, t.reb, t.ast) :: season_stats --IF yestday is null
            ]
        WHEN t.season IS NOT NULL 
            THEN y.season_stats || ARRAY[
                (t.season, t.gp, t.pts, t.reb, t.ast) :: season_stats --if today is not null we create the new value
            ]
        ELSE y.season_stats -- we carry the history
        END AS season_stats,
        COALESCE(t.season, y.current_season+1) AS current_season 
    FROM today AS t
    FULL OUTER JOIN yesterday AS y
        ON t.player_name = y.player_name;


SELECT * FROM players
WHERE current_season = 2001
AND player_name = 'Michael Jordan';


--transform into player season
WITH unnested AS (
    SELECT player_name,
            UNNEST(season_stats) AS season_stats
    FROM players
    WHERE current_season = 2001
)
SELECT 
    player_name, 
    (season_stats :: season_stats).*
FROM unnested;



--analytics queries

CREATE TYPE scoring_class AS ENUM('star', 'good', 'average', 'bad');

DROP TABLE players;
CREATE TABLE players(
    player_name VARCHAR,
    height VARCHAR,
    college VARCHAR,
    country VARCHAR,
    draft_year VARCHAR,
    draft_round VARCHAR,
    draft_number VARCHAR,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);


INSERT INTO players
    WITH yesterday AS (
        SELECT * FROM players
        WHERE current_season = 2000
    ),

    today AS (
        SELECT * FROM player_seasons
        WHERE season = 2001
    )
    SELECT 
        COALESCE(t.player_name, y.player_name) AS player_name,
        COALESCE(t.height, y.height) AS height,
        COALESCE(t.college, y.college) AS college,
        COALESCE(t.country, y.country) AS country,
        COALESCE(t.draft_year, y.draft_year) AS draft_year,
        COALESCE(t.draft_round, y.draft_round) AS draft_round,
        COALESCE(t.draft_number, y.draft_number) AS draft_number,
        CASE WHEN y.season_stats IS NULL 
            THEN ARRAY[
                (t.season, t.gp, t.pts, t.reb, t.ast) :: season_stats --IF yestday is null
            ]
        WHEN t.season IS NOT NULL 
            THEN y.season_stats || ARRAY[
                (t.season, t.gp, t.pts, t.reb, t.ast) :: season_stats --if today is not null we create the new value
            ]
        ELSE y.season_stats -- we carry the history
        END AS season_stats,

        CASE WHEN t.season IS NOT NULL 
            THEN 
                (CASE WHEN t.pts > 20 THEN 'star'
                    WHEN t.pts > 15 THEN 'good'
                    WHEN t.pts > 10 THEN 'average'
                    ELSE 'bad'
                    END) :: scoring_class
            ELSE y.scoring_class
        END AS scoring_class,

        CASE WHEN t.season IS NOT NULL THEN 0 
            ELSE y.years_since_last_season + 1
        END AS years_since_last_season, -- cummulation of years active

        COALESCE(t.season, y.current_season+1) AS current_season 
    FROM today AS t
    FULL OUTER JOIN yesterday AS y
        ON t.player_name = y.player_name;

SELECT 
    * 
FROM players
WHERE current_season = 2001
AND player_name = 'Michael Jordan';


--players which have biggest improvement from 1st season to most recent season

SELECT 
    player_name,
    (season_stats[CARDINALITY(season_stats)]::season_stats).pts/ 
   CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END
FROM players
WHERE current_season = 2001
AND scoring_class = 'star'
ORDER BY 2 DESC;


--This has no group by , it is insanely fast 


-- Cumulative Table design updated to handle till 2022
DROP TABLE players;
CREATE TABLE players(
    player_name VARCHAR,
    height VARCHAR,
    college VARCHAR,
    country VARCHAR,
    draft_year VARCHAR,
    draft_round VARCHAR,
    draft_number VARCHAR,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (player_name, current_season)
);

INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_season,
    w.season AS current_season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = w.season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;
SELECT * FROM players
WHERE player_name = 'Michael Jordan';
