-- ============================================================================
-- 03_1_generate_device_activity_datelist.sql
-- Cumulative query to populate user_devices_cumulated (Incremental Pattern)
-- ============================================================================

INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist, date)

WITH yesterday AS (
    -- Get previous day's cumulative data
    SELECT
        user_id,
        browser_type,
        device_activity_datelist
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-01') - INTERVAL '1 day'  -- Previous day
),

today AS (
    -- Get today's new activity
    SELECT DISTINCT
        e.user_id::TEXT,
        d.browser_type,
        DATE(e.event_time) as today_date
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE(e.event_time) = DATE('2023-01-02')  -- Current processing date
    AND e.user_id IS NOT NULL
),

combined AS (
    -- Combine yesterday's data with today's new activity
    SELECT
        COALESCE(y.user_id, t.user_id) as user_id,
        COALESCE(y.browser_type, t.browser_type) as browser_type,
        CASE
            -- User had activity yesterday AND today: append today to yesterday's array
            WHEN y.device_activity_datelist IS NOT NULL AND t.today_date IS NOT NULL THEN
                ARRAY_APPEND(y.device_activity_datelist, t.today_date)

            -- User had activity yesterday but NOT today: keep yesterday's array unchanged
            WHEN y.device_activity_datelist IS NOT NULL AND t.today_date IS NULL THEN
                y.device_activity_datelist

            -- User had NO activity yesterday but HAS activity today: create new array
            WHEN y.device_activity_datelist IS NULL AND t.today_date IS NOT NULL THEN
                ARRAY[t.today_date]

        END as device_activity_datelist
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.user_id = t.user_id
        AND y.browser_type = t.browser_type
)

SELECT
    user_id,
    browser_type,
    device_activity_datelist,
    DATE('2023-01-02') as date  -- Current processing date
FROM combined
WHERE device_activity_datelist IS NOT NULL  -- Only include users with activity
ORDER BY user_id, browser_type;