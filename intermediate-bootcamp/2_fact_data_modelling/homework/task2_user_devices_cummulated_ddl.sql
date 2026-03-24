-- Task 2: Cumulative user×device state — PK (user, browser, curr_date); date array holds all active days through curr_date.
DROP TABLE IF EXISTS user_device_cummulated;

CREATE TABLE user_device_cummulated(
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
    curr_date DATE,
    PRIMARY KEY(user_id,browser_type,curr_date)
)