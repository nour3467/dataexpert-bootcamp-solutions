-- ============================================================================
-- Automated processing for entire month (January 2023)
-- This will process all days in one go
-- ============================================================================

DO $$
DECLARE
    process_date DATE;
BEGIN
    -- Loop through each day in January 2023
    FOR process_date IN
        SELECT generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)::DATE
    LOOP
        RAISE NOTICE 'Processing date: %', process_date;

        INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist, date)

        WITH yesterday AS (
            -- Get previous day's cumulative data
            SELECT
                user_id,
                browser_type,
                device_activity_datelist
            FROM user_devices_cumulated
            WHERE date = process_date - INTERVAL '1 day'
        ),

        today AS (
            -- Get today's new activity
            SELECT DISTINCT
                e.user_id::TEXT,
                d.browser_type,
                DATE(e.event_time) as today_date
            FROM events e
            JOIN devices d ON e.device_id = d.device_id
            WHERE DATE(e.event_time) = process_date
            AND e.user_id IS NOT NULL
        ),

        combined AS (
            -- Combine yesterday's data with today's new activity
            SELECT
                COALESCE(y.user_id, t.user_id) as user_id,
                COALESCE(y.browser_type, t.browser_type) as browser_type,
                CASE
                    WHEN y.device_activity_datelist IS NOT NULL AND t.today_date IS NOT NULL THEN
                        ARRAY_APPEND(y.device_activity_datelist, t.today_date)
                    WHEN y.device_activity_datelist IS NOT NULL AND t.today_date IS NULL THEN
                        y.device_activity_datelist
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
            process_date as date  -- Use loop variable
        FROM combined
        WHERE device_activity_datelist IS NOT NULL
        ORDER BY user_id, browser_type;

        -- Show progress
        RAISE NOTICE 'Inserted % records for %',
            (SELECT COUNT(*) FROM user_devices_cumulated WHERE date = process_date),
            process_date;

    END LOOP;
END $$;