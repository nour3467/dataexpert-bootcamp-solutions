-- ============================================================================
-- 07_load_host_activity_reduced.sql
-- Incremental day-by-day loading of host_activity_reduced table
-- Following the pattern from generate_monthly_array_metrics.sql
-- ============================================================================

INSERT INTO host_activity_reduced (month_start, host, hit_array, unique_visitors_array, last_updated)

WITH yesterday AS (
    -- Get existing month data for current month
    SELECT
        month_start,
        host,
        hit_array,
        unique_visitors_array
    FROM host_activity_reduced
    WHERE month_start = DATE_TRUNC('month', DATE('2023-01-22'))  -- Current month
),

today AS (
    -- Get today's aggregated host activity
    SELECT
        e.host,
        DATE_TRUNC('month', DATE(e.event_time::timestamp)) as month_start,
        DATE(e.event_time::timestamp) as today_date,
        COUNT(1) as daily_hits,                           -- Hit count for today
        COUNT(DISTINCT e.user_id) as daily_unique_visitors -- Unique visitor count for today
    FROM events e
    WHERE DATE(e.event_time::timestamp) = DATE('2023-01-22')  -- Current processing date
    AND e.user_id IS NOT NULL
    AND e.host IS NOT NULL
    GROUP BY e.host, DATE_TRUNC('month', DATE(e.event_time::timestamp)), DATE(e.event_time::timestamp)
),

combined AS (
    -- Combine yesterday's arrays with today's new data
    SELECT
        COALESCE(y.month_start, t.month_start) as month_start,
        COALESCE(y.host, t.host) as host,

        -- Update hit_array: append today's hits or create new array
        CASE
            -- Existing month record: append today's hits to array
            WHEN y.hit_array IS NOT NULL THEN
                y.hit_array || ARRAY[t.daily_hits]

            -- New month record: pad array with NULLs up to today, then add today's hits
            WHEN y.hit_array IS NULL AND t.daily_hits IS NOT NULL THEN
                ARRAY_FILL(NULL::INTEGER, ARRAY[EXTRACT(DAY FROM t.today_date)::INTEGER - 1])
                || ARRAY[t.daily_hits]

            -- Edge case: no activity today, keep yesterday's array
            ELSE y.hit_array
        END as hit_array,

        -- Update unique_visitors_array: same logic as hit_array
        CASE
            -- Existing month record: append today's unique visitors to array
            WHEN y.unique_visitors_array IS NOT NULL THEN
                y.unique_visitors_array || ARRAY[t.daily_unique_visitors]

            -- New month record: pad array with NULLs up to today, then add today's unique visitors
            WHEN y.unique_visitors_array IS NULL AND t.daily_unique_visitors IS NOT NULL THEN
                ARRAY_FILL(NULL::INTEGER, ARRAY[EXTRACT(DAY FROM t.today_date)::INTEGER - 1])
                || ARRAY[t.daily_unique_visitors]

            -- Edge case: no activity today, keep yesterday's array
            ELSE y.unique_visitors_array
        END as unique_visitors_array,

        NOW() as last_updated
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.host = t.host
        AND y.month_start = t.month_start
)

SELECT
    month_start,
    host,
    hit_array,
    unique_visitors_array,
    last_updated
FROM combined
WHERE hit_array IS NOT NULL OR unique_visitors_array IS NOT NULL

-- Use UPSERT to handle conflicts (rerunning same day)
ON CONFLICT (month_start, host)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array,
    last_updated = EXCLUDED.last_updated;

-- ============================================================================
-- Verification queries (run after INSERT)
-- ============================================================================

-- Check today's results
SELECT
    month_start,
    host,
    array_length(hit_array, 1) as days_in_array,
    hit_array[array_length(hit_array, 1)] as todays_hits,  -- Last element = today
    unique_visitors_array[array_length(unique_visitors_array, 1)] as todays_unique_visitors,
    (SELECT SUM(unnested) FROM unnest(hit_array) AS unnested WHERE unnested IS NOT NULL) as total_month_hits
FROM host_activity_reduced
WHERE month_start = DATE_TRUNC('month', DATE('2023-01-22'))
ORDER BY todays_hits DESC
LIMIT 10;

-- Check array consistency (both arrays should have same length)
SELECT
    month_start,
    host,
    array_length(hit_array, 1) as hit_array_length,
    array_length(unique_visitors_array, 1) as visitors_array_length,
    CASE
        WHEN array_length(hit_array, 1) = array_length(unique_visitors_array, 1) THEN 'OK'
        ELSE 'MISMATCH'
    END as array_consistency
FROM host_activity_reduced
WHERE month_start = DATE_TRUNC('month', DATE('2023-01-22'))
AND (array_length(hit_array, 1) != array_length(unique_visitors_array, 1)
     OR array_length(hit_array, 1) IS NULL
     OR array_length(unique_visitors_array, 1) IS NULL);

-- ============================================================================
-- Example: Run for multiple days automatically
-- ============================================================================

/*
-- To process entire month, wrap in a loop:
DO $$
DECLARE
    process_date DATE;
BEGIN
    FOR process_date IN
        SELECT generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)::DATE
    LOOP
        RAISE NOTICE 'Processing host activity for date: %', process_date;

        -- Insert the above query here, replacing DATE('2023-01-22') with process_date
        -- ... (full INSERT statement)

    END LOOP;
END $$;
*/