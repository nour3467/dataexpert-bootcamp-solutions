SELECT COUNT(*) as total_sessions
FROM user_sessions;


SELECT
    host,
    COUNT(*) as session_count
FROM user_sessions
GROUP BY host
ORDER BY session_count DESC;

-- QUESTION 1: Average events per session on Tech Creator
SELECT
    COUNT(*) as total_sessions,
    SUM(num_events) as total_events,
    ROUND(AVG(num_events)::NUMERIC, 2) as avg_events_per_session
FROM user_sessions
WHERE host LIKE '%dataexpert%'; -- techcreator :: THE SAME CONCEPT

-- QUESTION 2: Compare specific hosts
SELECT
    host,
    COUNT(*) as total_sessions,
    ROUND(AVG(num_events::NUMERIC), 2) as avg_events_per_session
FROM user_sessions
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;


-- Compare ALL hosts you have data for
SELECT
    host,
    COUNT(*) as total_sessions,
    ROUND(AVG(num_events::NUMERIC), 2) as avg_events_per_session
FROM user_sessions
GROUP BY host
HAVING COUNT(*) >= 2  -- Only hosts with 2+ sessions
ORDER BY avg_events_per_session DESC;


SELECT
    COUNT(*) as total_sessions,
    MIN(session_start) as first_session,
    MAX(session_start) as latest_session
FROM user_sessions;