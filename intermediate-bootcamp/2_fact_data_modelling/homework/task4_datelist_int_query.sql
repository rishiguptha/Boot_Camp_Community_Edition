-- Task 4: Convert DATE[] datelist to a 32-bit integer bitmap — each set bit marks a day the user was active within the 30-day window.
WITH
    users AS (
        SELECT *
        FROM user_device_cummulated
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
                -- @> is array containment; true if the user was active on series_date.
                WHEN device_activity_datelist @> ARRAY[DATE (series_date)] THEN CAST(
                    POW (
                        2,
                        32 - (
                            DATE (curr_date::TIMESTAMP) - DATE (series_date)
                        ) -- bit position: most recent day = bit 32, oldest = bit 1
                    ) AS BIGINT
                )
                ELSE 0
            END AS placeholder_int_value,
            *
        FROM users
            CROSS JOIN series
    )
SELECT user_id, browser_type, (
        SUM(placeholder_int_value)::BIGINT
    )::BIT(32) AS datelist_int
FROM place_holder_ints
GROUP BY
    user_id,
    browser_type;