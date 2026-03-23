-- Explore raw event bounds before modeling user activity state.
SELECT * FROM events;

-- Confirm available event-time window to anchor snapshot and bit-range choices.
SELECT MAX(event_time), MIN(event_time) FROM events;

-- Reset the demo table so the script can be rerun from a clean state.
DROP TABLE users_cummulated;

-- Daily snapshot table: one row per user per day with their full activity history up to that day.
CREATE TABLE users_cummulated (
    user_id TEXT, -- keep as text here; switch to BIGINT in production when source IDs are guaranteed numeric.
    dates_active DATE[], -- compact cumulative history used to derive engagement windows without rescanning events.
    curr_date DATE, -- snapshot date this cumulative state represents.
    PRIMARY KEY (user_id, curr_date)
);

INSERT INTO
    users_cummulated
WITH
    yesterday AS (
        -- Previous snapshot is the base state for incremental processing (no full backfill each run).
        SELECT *
        FROM users_cummulated
        WHERE
            curr_date = DATE ('2023-01-30')
    ),
    today AS (
        -- One record per active user-day; null user_ids are excluded to protect downstream aggregates.
        SELECT user_id::TEXT, DATE (event_time::TIMESTAMP) AS date_active
        FROM events
        WHERE
            DATE (event_time::TIMESTAMP) = DATE ('2023-01-31')
            AND user_id IS NOT NULL
        GROUP BY
            user_id,
            DATE (event_time::TIMESTAMP)
    )
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
    -- Handle three states: brand-new user, inactive today, or active today (prepend newest date).
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(
        t.date_active,
        y.curr_date + INTERVAL '1 day'
    ) AS curr_date
FROM today t
    -- FULL OUTER JOIN preserves both retained users and newly active users.
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;

-- Inspect the newly built daily snapshot.
SELECT * FROM users_cummulated WHERE curr_date = '2023-01-31';

-- Preview the generated date backbone used for bitmap encoding.
SELECT *
FROM generate_series(
        DATE ('2023-01-02'), DATE ('2023-01-31'), INTERVAL '1 day'
    );

WITH
    users AS (
        -- Fix to a single snapshot date so bit positions are deterministic for all users.
        SELECT *
        FROM users_cummulated
        WHERE
            curr_date = DATE ('2023-01-31')
    ),
    series AS (
        -- Generate the analysis window that will map directly onto 32 bits.
        SELECT *
        FROM generate_series(
                DATE ('2023-01-02'), DATE ('2023-01-31'), INTERVAL '1 day'
            ) AS series_date
    ),
    place_holder_ints AS (
        SELECT
            CASE
            -- Membership test: did this user appear on this exact day?
                WHEN dates_active @> ARRAY[DATE (series_date)] THEN CAST(
                    POW (
                        -- Power-of-two creates a single-bit flag.
                        2,
                        32 - (
                            -- Offset from snapshot date controls bit position (recent days -> higher-order bits).
                            DATE (curr_date::TIMESTAMP) - DATE (series_date)
                        )
                    ) AS BIGINT
                )
                -- Inactive days explicitly map to 0 so they do not affect the final SUM.
                ELSE 0
            END AS placeholder_int_value,
            *
        FROM users
            -- Intentional cartesian product: evaluate every user against every day in the window.
            CROSS JOIN series
    )
    -- Final artifact: per-user 32-bit activity vector for fast DAU/WAU/MAU-style bitwise analytics.
SELECT
    user_id,
    -- Combine per-day powers of two into one compact 32-day engagement bitmap.
    (
        SUM(placeholder_int_value)::BIGINT
    )::BIT(32),
    -- MAU proxy: any bit set in the 32-day window means active this month.
    BIT_COUNT(
        (
            SUM(placeholder_int_value)::BIGINT
        )::BIT(32)
    ) > 0 AS dim_is_monthly_active,
    -- WAU proxy: mask the most recent 7 days, then check if any activity remains.
    BIT_COUNT(
        CAST(
            '11111110000000000000000000000000' AS BIT(32)
        ) & (
            SUM(placeholder_int_value)::BIGINT
        )::BIT(32)
    ) > 0 AS dim_is_weekly_active,
    -- DAU proxy: check only the latest-day bit.
    BIT_COUNT(
        CAST(
            '10000000000000000000000000000000' AS BIT(32)
        ) & (
            SUM(placeholder_int_value)::BIGINT
        )::BIT(32)
    ) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY
    user_id;