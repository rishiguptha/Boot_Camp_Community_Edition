-- Task 7: Reduced fact table at month×host grain — parallel integer arrays store daily hit counts and unique visitors indexed by day-of-month.
DROP TABLE IF EXISTS host_activity_reduced;

CREATE TABLE host_activity_reduced(
    month DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY(
        host,
        month
    )
)