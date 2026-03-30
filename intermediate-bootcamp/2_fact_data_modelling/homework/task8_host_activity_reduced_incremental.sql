-- Task 8: Incremental reduced fact — append today's metrics as the next array element; ARRAY_FILL pads zeros for hosts appearing mid-month.
INSERT INTO
    host_activity_reduced
WITH
    daily_aggregate AS (
        SELECT
            host,
            DATE (event_time) AS date,
            COUNT(1) AS num_site_hits,
            COUNT(DISTINCT user_id) AS unique_visitor
        FROM events
        WHERE
            DATE (event_time) = DATE ('2023-01-03')
            AND host IS NOT NULL
        GROUP BY
            host,
            DATE (event_time)
    ),
    yesterday_array AS (
        SELECT *
        FROM host_activity_reduced
        WHERE
            month = DATE ('2023-01-01')
    )
SELECT
    COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month,
    COALESCE(da.host, ya.host) AS host,
    CASE
        WHEN da.host IS NULL THEN ya.hit_array
        WHEN ya.hit_array IS NOT NULL THEN CASE
            WHEN ARRAY_LENGTH(ya.hit_array, 1) >= EXTRACT(DAY FROM da.date)::INT THEN ya.hit_array
            ELSE ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        END
        ELSE CASE
            WHEN EXTRACT(DAY FROM da.date)::INT > 1 THEN ARRAY_FILL(
                0,
                ARRAY[(EXTRACT(DAY FROM da.date)::INT - 1)]
            )
            ELSE ARRAY[]::INTEGER[]
            END || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS hit_array,
    CASE
        WHEN da.host IS NULL THEN ya.unique_visitors
        WHEN ya.unique_visitors IS NOT NULL THEN CASE
            WHEN ARRAY_LENGTH(ya.unique_visitors, 1) >= EXTRACT(DAY FROM da.date)::INT THEN ya.unique_visitors
            ELSE ya.unique_visitors || ARRAY[COALESCE(da.unique_visitor, 0)]
        END
        ELSE CASE
            WHEN EXTRACT(DAY FROM da.date)::INT > 1 THEN ARRAY_FILL(
                0,
                ARRAY[(EXTRACT(DAY FROM da.date)::INT - 1)]
            )
            ELSE ARRAY[]::INTEGER[]
            END || ARRAY[COALESCE(da.unique_visitor, 0)]
    END AS unique_visitors
FROM
    daily_aggregate da
    FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
ON CONFLICT (host, month) DO
UPDATE
SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;


SELECT * FROM host_activity_reduced;
