-- Task 5: Cumulative host state — PK (host, curr_date); date array accumulates every day the host served traffic through curr_date.
DROP TABLE IF EXISTS hosts_cumulated;

CREATE TABLE hosts_cumulated(
    host TEXT NOT NULL,
    host_activity_datelist DATE[] NOT NULL,
    curr_date DATE NOT NULL,
    PRIMARY KEY(host, curr_date)
);
