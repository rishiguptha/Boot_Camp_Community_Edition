-- Task 7: Reduced fact table at month×host grain — parallel integer arrays store daily hit counts and unique visitors indexed by day-of-month.
DROP TABLE IF EXISTS host_activity_reduced;

CREATE TABLE host_activity_reduced(
    month DATE NOT NULL,
    host TEXT NOT NULL,
    hit_array INTEGER[] NOT NULL,
    unique_visitors INTEGER[] NOT NULL,
    PRIMARY KEY(
        host,
        month
    ),
    CHECK (month = DATE_TRUNC('month', month)::DATE),
    CHECK (
        COALESCE(ARRAY_LENGTH(hit_array, 1), 0) = COALESCE(ARRAY_LENGTH(unique_visitors, 1), 0)
    )
);
