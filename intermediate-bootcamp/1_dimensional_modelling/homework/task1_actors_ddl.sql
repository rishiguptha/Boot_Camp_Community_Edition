-- TASK - 1
-- Creating Struct (film_struct) for ARRAY[struct]
DROP TYPE IF EXISTS film_struct CASCADE;

CREATE TYPE film_struct AS(
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- CREATING ENUM for quality_class
DROP TYPE IF EXISTS quality_class CASCADE;
CREATE TYPE quality_class AS ENUM(
    'star',
    'good',
    'average',
    'bad'
);
-- DDL for actors cummulative Table
DROP TABLE IF EXISTS actors;
CREATE TABLE actors(
    actorid TEXT,
    actor TEXT,
    as_of_year INTEGER,
    films film_struct [],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY(actorid, as_of_year)
);