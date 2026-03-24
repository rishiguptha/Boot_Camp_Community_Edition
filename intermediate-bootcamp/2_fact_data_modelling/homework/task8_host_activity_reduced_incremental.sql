-- Task 8: Incremental reduced fact — append today's metrics as the next array element; ARRAY_FILL pads zeros for hosts appearing mid-month.
TRUNCATE TABLE host_activity_reduced;

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
    COALESCE(
        ya.month,
        DATE_TRUNC('month', da.date)
    ) AS month,
    COALESCE(da.host, ya.host) AS host,
    CASE
        WHEN ya.hit_array IS NOT NULL THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.month IS NULL THEN ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.hit_array IS NULL THEN ARRAY_FILL(
            0,
            ARRAY[
                COALESCE(
                    date - DATE (DATE_TRUNC('month', date)),
                    0
                ) -- pad zeros for days 1..(today-1) when host first appears mid-month
            ]
        ) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS hit_array,
    CASE
        WHEN ya.unique_visitors IS NOT NULL THEN ya.unique_visitors || ARRAY[
            COALESCE(da.unique_visitor, 0)
        ]
        WHEN ya.month IS NULL THEN ARRAY[
            COALESCE(da.unique_visitor, 0)
        ]
        WHEN ya.unique_visitors IS NULL THEN ARRAY_FILL(
            0,
            ARRAY[
                COALESCE(
                    date - DATE (DATE_TRUNC('month', date)),
                    0
                )
            ]
        ) || ARRAY[
            COALESCE(da.unique_visitor, 0)
        ]
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