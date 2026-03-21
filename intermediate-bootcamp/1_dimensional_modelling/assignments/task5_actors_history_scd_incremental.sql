-- Task 5: Incremental SCD — snapshot year y built from prior table state + actors for as_of_year = y.
-- Run type creation once per DB; comment out DROP/CREATE if actors_scd_type already exists.
DROP TYPE IF EXISTS actors_scd_type CASCADE;

CREATE TYPE actors_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);

WITH
    params AS (
        SELECT 2022::int AS y
    ),
    last_year_scd AS (
        SELECT *
        FROM actors_history_scd
        WHERE
            current_year = (
                SELECT y - 1
                FROM params
            )
            AND end_year = (
                SELECT y - 1
                FROM params
            )
    ),
    historical_scd AS (
        SELECT
            actorid,
            actor,
            quality_class,
            is_active,
            start_year,
            end_year,
            (
                SELECT y
                FROM params
            ) AS current_year
        FROM actors_history_scd
        WHERE
            current_year = (
                SELECT y - 1
                FROM params
            )
            AND end_year < (
                SELECT y - 1
                FROM params
            )
    ),
    this_year_actors AS (
        SELECT *
        FROM actors
        WHERE
            as_of_year = (
                SELECT y
                FROM params
            )
    ),
    unchanged_records AS (
        SELECT
            ty.actorid,
            ty.actor,
            ty.quality_class,
            ty.is_active,
            ly.start_year,
            ty.as_of_year AS end_year,
            (
                SELECT y
                FROM params
            ) AS current_year
        FROM
            this_year_actors AS ty
            JOIN last_year_scd AS ly ON ty.actorid = ly.actorid
        WHERE
            ty.quality_class IS NOT DISTINCT FROM ly.quality_class
            AND ty.is_active IS NOT DISTINCT FROM ly.is_active
    ),
    changed_records AS (
        SELECT ty.actorid, ty.actor, UNNEST(
                ARRAY[
                    ROW (
                        ly.quality_class, ly.is_active, ly.start_year, (
                            SELECT y - 1
                            FROM params
                        )
                    )::actors_scd_type, ROW (
                        ty.quality_class, ty.is_active, ty.as_of_year, ty.as_of_year
                    )::actors_scd_type
                ]
            ) AS records
        FROM
            this_year_actors ty
            INNER JOIN last_year_scd ly ON ty.actorid = ly.actorid
        WHERE
            ty.quality_class IS DISTINCT FROM ly.quality_class
            OR ty.is_active IS DISTINCT FROM ly.is_active
    ),
    unnested_changed_records AS (
        SELECT
            actorid,
            actor,
            (records::actors_scd_type).quality_class,
            (records::actors_scd_type).is_active,
            (records::actors_scd_type).start_year,
            (records::actors_scd_type).end_year,
            (
                SELECT y
                FROM params
            ) AS current_year
        FROM changed_records
    ),
    new_records AS (
        SELECT
            ty.actorid,
            ty.actor,
            ty.quality_class,
            ty.is_active,
            ty.as_of_year AS start_year,
            ty.as_of_year AS end_year,
            (
                SELECT y
                FROM params
            ) AS current_year
        FROM
            this_year_actors ty
            LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
        WHERE
            ly.actorid IS NULL
    ),
    retained_records AS (
        SELECT ly.actorid, ly.actor, ly.quality_class, ly.is_active, ly.start_year, ly.end_year, (
                SELECT y
                FROM params
            ) AS current_year
        FROM
            last_year_scd ly
            LEFT JOIN this_year_actors ty ON ly.actorid = ty.actorid
        WHERE
            ty.actorid IS NULL
    )
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM historical_scd
UNION ALL
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM unchanged_records
UNION ALL
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM unnested_changed_records
UNION ALL
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM new_records
UNION ALL
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM retained_records;