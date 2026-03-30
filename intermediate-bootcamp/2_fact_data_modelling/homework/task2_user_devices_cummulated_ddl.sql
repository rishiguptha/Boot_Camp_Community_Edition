-- Task 2: Cumulative user×device state — PK (user, browser, curr_date); date array holds all active days through curr_date.
DROP TABLE IF EXISTS user_devices_cumulated;

CREATE TABLE user_devices_cumulated(
    user_id BIGINT NOT NULL,
    browser_type TEXT NOT NULL,
    device_activity_datelist DATE[] NOT NULL,
    curr_date DATE NOT NULL,
    PRIMARY KEY(user_id, browser_type, curr_date)
);
