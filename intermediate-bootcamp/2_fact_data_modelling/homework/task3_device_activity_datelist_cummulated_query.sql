-- Task 3: Incremental datelist cumulative — merge prior snapshot with today’s events so dormant keys keep their history.
TRUNCATE TABLE user_device_cummulated;


INSERT INTO user_device_cummulated
WITH
    yesterday AS (
        SELECT *
        FROM user_device_cummulated
        WHERE
            curr_date = DATE ('2023-01-30')
    ),
    today AS (
        SELECT e.user_id, d.browser_type, DATE (event_time::TIMESTAMP) AS date_active
        FROM events e
            LEFT JOIN devices d ON e.device_id = d.device_id
        WHERE
            DATE (event_time::TIMESTAMP) = DATE ('2023-01-31')
            AND e.user_id IS NOT NULL
            AND d.browser_type IS NOT NULL
        GROUP BY
            e.user_id,
            d.browser_type,
            DATE (event_time::TIMESTAMP)
    )
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(
        t.browser_type,
        y.browser_type
    ) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(
        t.date_active,
        y.curr_date + INTERVAL '1 day'
    ) AS curr_date
FROM today t
    -- Full outer union: new keys without yesterday vs. keys with no event today still advance curr_date.
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
    AND t.browser_type = y.browser_type;


SELECT * FROM user_device_cummulated