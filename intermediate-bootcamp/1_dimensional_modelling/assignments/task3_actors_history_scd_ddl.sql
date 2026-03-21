-- TASK - 3
DROP TABLE IF EXISTS actors_history_scd;  
CREATE TABLE actors_history_scd(
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    current_year INTEGER,
    PRIMARY KEY(current_year, actorid, start_year)
);