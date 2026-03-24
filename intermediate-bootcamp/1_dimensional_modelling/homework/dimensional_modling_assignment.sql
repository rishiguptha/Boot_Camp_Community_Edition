-- Checking the actor_films
SELECT *
FROM actor_films;

-- Creating Struct (film_struct) for ARRAY[struct]
CREATE TYPE film_struct AS(
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- CREATING ENUM for quality_class
CREATE TYPE quality_class AS ENUM(
    'star',
    'good',
    'average',
    'bad'
);
-- DDL for actors cummulative Table
DROP TABLE actors;
CREATE TABLE actors(
    actorid TEXT,
    actor TEXT,
    as_of_year INTEGER,
    films film_struct [],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY(actorid, as_of_year)
);

-- -- Seed 1970 snapshot from 1969 actors + 1970 films (incremental example)
-- INSERT INTO actors
--     WITH last_year AS (
--         SELECT *
--         FROM actors
--         WHERE as_of_year = 1969
--     ),
--     this_year AS (
--         SELECT actorid,
--             actor,
--             ARRAY_AGG((film, votes, rating, filmid)::film_struct) AS this_year_films,
--             AVG(rating) AS avg_rating_this_year
--         FROM actor_films
--         WHERE year = 1970
--         GROUP BY actorid,
--             actor
--     )
-- SELECT COALESCE(t.actorid, l.actorid) AS actorid,
--     COALESCE(t.actor, l.actor) AS actor,
--     1970 AS as_of_year,
--     CASE
--         WHEN l.films IS NULL
--         AND t.this_year_films IS NOT NULL THEN t.this_year_films
--         WHEN l.films IS NOT NULL
--         AND t.this_year_films IS NULL THEN l.films
--         WHEN l.films IS NOT NULL
--         AND t.this_year_films IS NOT NULL THEN l.films || t.this_year_films
--         ELSE NULL
--     END AS films,
--     CASE
--         WHEN t.this_year_films IS NOT NULL THEN (
--             CASE
--                 WHEN t.avg_rating_this_year > 8 THEN 'star'
--                 WHEN t.avg_rating_this_year > 7 THEN 'good'
--                 WHEN t.avg_rating_this_year > 6 THEN 'average'
--                 ELSE 'bad'
--             END
--         )::quality_class
--         ELSE l.quality_class
--     END AS quality_class,
--     CASE
--         WHEN t.this_year_films IS NOT NULL THEN true
--         ELSE false
--     END AS is_active
-- FROM this_year t
--     FULL OUTER JOIN last_year l ON t.actorid = l.actorid;
    
    
    

-- Windowed approach to build the full actors cumulative table in one shot
TRUNCATE TABLE actors;
INSERT INTO actors 
    WITH years AS (
        SELECT GENERATE_SERIES(1970, 2021) AS as_of_year
    ),
    -- First year each actor appears in actor_films
    actors_first_year AS (
        SELECT actorid,
            actor,
            MIN(year) AS first_year
        FROM actor_films
        GROUP BY actorid,
            actor
    ),
    -- One row per actor per year from their first_year through 2021
    actor_years AS (
        SELECT a.actorid,
            a.actor,
            y.as_of_year
        FROM actors_first_year a
            JOIN years y ON y.as_of_year >= a.first_year
    ),
    -- Attach films (if any) for each actor/year combination
    actor_year_films AS (
        SELECT ay.actorid,
            ay.actor,
            ay.as_of_year,
            af.film,
            af.votes,
            af.rating,
            af.filmid
        FROM actor_years ay
            LEFT JOIN actor_films af ON af.actorid = ay.actorid
            AND af.year = ay.as_of_year
    ),
    -- Cumulative films array per actor/year using windowed ARRAY_AGG
    windowed_films AS (
        SELECT actorid,
            actor,
            as_of_year,
            ARRAY_REMOVE(
                ARRAY_AGG(
                    CASE
                        WHEN filmid IS NOT NULL THEN (film, votes, rating, filmid)::film_struct
                    END
                ) OVER (
                    PARTITION BY actorid
                    ORDER BY as_of_year
                ),
                NULL
            ) AS films
        FROM actor_year_films
    ),
    -- is_active flag: true if actor has at least one film in that year
    actor_year_active AS (
        SELECT actorid,
            as_of_year,
            COUNT(filmid) > 0 AS is_active
        FROM actor_year_films
        GROUP BY actorid,
            as_of_year
    ),
    -- Per-actor, per-year average film rating
    actor_year_avg_rating AS (
        SELECT actorid,
            year AS film_year,
            AVG(rating) as avg_rating
        FROM actor_films
        GROUP BY actorid,
            year
    ),
    -- Map avg_rating for each actor/year into quality_class bucket
    actor_year_quality AS (
        SELECT ay.actorid,
            ay.as_of_year,
            avg_rating,
            CASE
                WHEN avg_rating IS NULL THEN NULL
                ELSE(
                    CASE
                        WHEN avg_rating > 8 THEN 'star'
                        WHEN avg_rating > 7 THEN 'good'
                        WHEN avg_rating > 6 THEN 'average'
                        ELSE 'bad'
                    END
                )::quality_class 
                END AS quality_class
                FROM actor_years ay
                    LEFT JOIN actor_year_avg_rating ar ON ay.actorid = ar.actorid
                    AND ay.as_of_year = ar.film_year
            ) 
-- Final actor snapshots: one row per actor/as_of_year with films, quality_class, is_active
SELECT w.actorid,
    w.actor,
    w.as_of_year,
    w.films,
    ayq.quality_class,
    aya.is_active
FROM windowed_films w
LEFT JOIN actor_year_active aya ON w.actorid = aya.actorid
AND w.as_of_year = aya.as_of_year
LEFT JOIN actor_year_quality ayq ON aya.actorid = ayq.actorid
AND w.as_of_year = ayq.as_of_year;


SELECT *
FROM actors
WHERE as_of_year = 2021;