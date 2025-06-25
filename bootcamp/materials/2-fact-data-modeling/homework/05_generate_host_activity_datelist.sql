-- ============================================================================
-- 05_generate_host_activity_datelist.sql
-- Cumulative query to populate host_activity_cumulated (Incremental Pattern)
-- Assumes table already exists from separate DDL script
-- ============================================================================

INSERT INTO host_activity_cumulated (user_id, host_name, host_activity_datelist, date)

WITH yesterday AS (
    -- Get previous day's cumulative data
    SELECT
        user_id,
        host_name,
        host_activity_datelist
    FROM host_activity_cumulated
    WHERE date = DATE('2023-01-22') - INTERVAL '1 day'  -- Previous day
),

today AS (
    -- Get today's new host activity
    SELECT DISTINCT
        e.user_id::TEXT,
        e.host as host_name,  -- Use the actual 'host' column from events table
        DATE(e.event_time::timestamp) as today_date
    FROM events e
    WHERE DATE(e.event_time::timestamp) = DATE('2023-01-22')  -- Current processing date
    AND e.user_id IS NOT NULL
    AND e.host IS NOT NULL  -- Must have host info
),

combined AS (
    -- Combine yesterday's data with today's new activity
    SELECT
        COALESCE(y.user_id, t.user_id) as user_id,
        COALESCE(y.host_name, t.host_name) as host_name,
        CASE
            -- User had activity on this host yesterday AND today: append today to yesterday's array
            WHEN y.host_activity_datelist IS NOT NULL AND t.today_date IS NOT NULL THEN
                ARRAY_APPEND(y.host_activity_datelist, t.today_date)

            -- User had activity on this host yesterday but NOT today: keep yesterday's array unchanged
            WHEN y.host_activity_datelist IS NOT NULL AND t.today_date IS NULL THEN
                y.host_activity_datelist

            -- User had NO activity on this host yesterday but HAS activity today: create new array
            WHEN y.host_activity_datelist IS NULL AND t.today_date IS NOT NULL THEN
                ARRAY[t.today_date]

        END as host_activity_datelist
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.user_id = t.user_id
        AND y.host_name = t.host_name
)

SELECT
    user_id,
    host_name,
    host_activity_datelist,
    DATE('2023-01-22') as date  -- Current processing date
FROM combined
WHERE host_activity_datelist IS NOT NULL  -- Only include users with activity
ORDER BY user_id, host_name;