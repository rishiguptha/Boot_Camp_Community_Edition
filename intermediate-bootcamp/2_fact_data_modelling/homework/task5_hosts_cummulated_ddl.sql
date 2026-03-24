-- Task 5: Cumulative host state — PK (host, curr_date); date array accumulates every day the host served traffic through curr_date.
DROP TABLE IF EXISTS hosts_cummulated;

CREATE TABLE hosts_cummulated(
    host TEXT,
    host_activity_datelist DATE[],
    curr_date DATE,
    PRIMARY KEY(host,curr_date)
)