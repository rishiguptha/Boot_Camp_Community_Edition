-- Task 6: Incremental host datelist cumulation — same pattern as user×device but at host grain; no device join required.
INSERT INTO hosts_cumulated
WITH
    yesterday AS (
        SELECT *
        FROM hosts_cumulated
        WHERE
            curr_date = DATE ('2022-12-31')
    ),
    today AS (
        SELECT host, DATE (event_time::TIMESTAMP) AS date_active
        FROM events
        WHERE
            DATE (event_time::TIMESTAMP) = DATE ('2023-01-01')
            AND host IS NOT NULL
        GROUP BY
            host,
            DATE (event_time::TIMESTAMP)
    )
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.host_activity_datelist
        WHEN y.host_activity_datelist[1] = t.date_active THEN y.host_activity_datelist
        ELSE ARRAY[t.date_active] || y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(
        t.date_active,
        y.curr_date + INTERVAL '1 day'
    ) AS curr_date
FROM today t
    -- Full outer join: dormant hosts keep their datelist; new hosts get a fresh array.
    FULL OUTER JOIN yesterday y ON t.host = y.host
ON CONFLICT (host, curr_date) DO UPDATE
SET
    host_activity_datelist = EXCLUDED.host_activity_datelist;


SELECT * FROM hosts_cumulated;
