-- Q1: Average events per session across Tech Creator
SELECT AVG(num_hits) AS avg_events_per_session
FROM processed_events_sessionized
WHERE host LIKE '%techcreator.io%';

-- Q2: Comparison across the three hosts
SELECT 
    host,
    COUNT(*) AS num_sessions,
    AVG(num_hits) AS avg_events_per_session
FROM processed_events_sessionized
WHERE host IN (
    'zachwilson.techcreator.io',
    'zachwilson.tech',
    'lulu.techcreator.io'
)
GROUP BY host
ORDER BY avg_events_per_session DESC;