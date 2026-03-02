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
--Seed 

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
ORDER BY 2 DESC;


--This ha sno group by , it is insanely fast 
