TRUNCATE TABLE actors_history_scd;
INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT actorid,
        actor,
        as_of_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (
            PARTITION BY actorid
            ORDER BY as_of_year
        ) AS previous_quality_class,
        LAG(is_active, 1) OVER (
            PARTITION BY actorid
            ORDER BY as_of_year
        ) AS previous_is_active
    FROM actors
    WHERE as_of_year <= 2021
),
with_indicators AS (
    SELECT *,
        CASE 
            WHEN previous_quality_class IS NULL OR previous_is_active IS NULL THEN 1
            WHEN quality_class IS DISTINCT FROM previous_quality_class THEN 1
            WHEN is_active IS DISTINCT FROM previous_is_active THEN 1
            ELSE 0
        END as change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
    SUM(change_indicator) OVER(
        PARTITION BY actorid
        ORDER BY as_of_year
    ) as streak_identifier
    FROM with_indicators
)
SELECT actorid,
    MAX(actor),
    quality_class,
    is_active,
    MIN(as_of_year) AS start_year,
    MAX(as_of_year) AS end_year,
    2021 AS current_year
FROM with_streaks
GROUP BY actorid, streak_identifier, quality_class, is_active
ORDER BY actorid, streak_identifier;

SELECT * FROM actors_history_scd
WHERE actor = 'Tom Cruise';
