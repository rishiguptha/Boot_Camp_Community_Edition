-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);


CREATE TABLE IF NOT EXISTS processed_events_aggregated(
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);


CREATE TABLE IF NOT EXISTS processed_events_aggregated_source(
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);


CREATE TABLE IF NOT EXISTS processed_events_sessionized(
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);