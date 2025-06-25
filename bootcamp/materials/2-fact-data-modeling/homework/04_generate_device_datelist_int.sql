-- ============================================================================
-- 04_generate_device_datelist_int.sql
-- Convert device_activity_datelist to datelist_int using BIT operations
-- Complete working script - ready to run
-- ============================================================================

-- Step 1: Create target table (run this first time only)
CREATE TABLE IF NOT EXISTS user_devices_datelist_int (
    user_id TEXT,
    browser_type TEXT,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

-- Step 2: Generate datelist_int from device_activity_datelist
WITH starter AS (
    -- Check if each day in January 2023 exists in device_activity_datelist
    SELECT
        uc.user_id,
        uc.browser_type,
        uc.device_activity_datelist @> ARRAY[DATE(d.valid_date)] AS is_active,
        EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since
    FROM user_devices_cumulated uc
    CROSS JOIN (
        SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
    ) AS d
    WHERE uc.date = DATE('2023-01-31')  -- Process latest snapshot only
),
bits AS (
    -- Convert to binary using powers of 2 (following your exact pattern)
    SELECT
        user_id,
        browser_type,
        SUM(CASE
            WHEN is_active THEN POW(2, 32 - days_since)
            ELSE 0
        END)::bigint::bit(32) AS datelist_int,
        DATE('2023-01-31') as date
    FROM starter
    GROUP BY user_id, browser_type
)

-- Step 3: Insert results
INSERT INTO user_devices_datelist_int (user_id, browser_type, datelist_int, date)
SELECT
    user_id,
    browser_type,
    datelist_int,
    date
FROM bits
ON CONFLICT (user_id, browser_type, date)
DO UPDATE SET datelist_int = EXCLUDED.datelist_int;

-- ============================================================================
-- Quick verification query - run this after the INSERT
-- ============================================================================

-- Check results
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT browser_type) as unique_browsers
FROM user_devices_datelist_int
WHERE date = DATE('2023-01-31');

-- Show sample results
SELECT
    user_id,
    browser_type,
    datelist_int,
    BIT_COUNT(datelist_int) as active_days_count
FROM user_devices_datelist_int
WHERE date = DATE('2023-01-31')
ORDER BY BIT_COUNT(datelist_int) DESC
LIMIT 10;