-- Task 4: Convert DATE[] datelist to a 30-bit integer bitmap — LSB = oldest day in window; bit 29 = curr_date.
WITH
    users AS (
        SELECT *
        FROM user_devices_cumulated
        WHERE
            curr_date = DATE ('2023-01-31')
    ),
    series AS (
        SELECT *
        FROM GENERATE_SERIES(
                DATE ('2023-01-02'), DATE ('2023-01-31'), INTERVAL '1 day'
            ) AS series_date
    ),
    place_holder_ints AS (
        SELECT
            CASE
                WHEN u.device_activity_datelist @> ARRAY[s.series_date::DATE] THEN (
                    1::BIGINT << (
                        29 - (u.curr_date - s.series_date::DATE)
                    )
                )
                ELSE 0::BIGINT
            END AS placeholder_int_value,
            u.user_id,
            u.browser_type
        FROM users u
            CROSS JOIN LATERAL GENERATE_SERIES(
                u.curr_date - 29,
                u.curr_date,
                INTERVAL '1 day'
            ) AS s(series_date)
    )
SELECT
    user_id,
    browser_type,
    SUM(placeholder_int_value)::BIGINT AS datelist_int
FROM place_holder_ints
GROUP BY
    user_id,
    browser_type;
