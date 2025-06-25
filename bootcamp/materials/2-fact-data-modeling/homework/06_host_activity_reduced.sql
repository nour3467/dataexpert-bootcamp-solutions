-- ============================================================================
-- 06_host_activity_reduced.sql
-- Monthly reduced fact table DDL for host activity analytics
-- ============================================================================

-- Drop table if exists (for testing)
-- DROP TABLE IF EXISTS host_activity_reduced;

CREATE TABLE host_activity_reduced (
    month_start DATE NOT NULL,              -- First day of the month (e.g., '2023-01-01')
    host TEXT NOT NULL,                     -- Host name from events.host
    hit_array INTEGER[],                    -- Daily hit counts [day1_hits, day2_hits, ..., day31_hits]
    unique_visitors_array INTEGER[],        -- Daily unique visitor counts [day1_unique, day2_unique, ..., day31_unique]
    last_updated TIMESTAMP DEFAULT NOW(),   -- When this record was last updated

    PRIMARY KEY (month_start, host)
);

-- ============================================================================
-- Indexes for performance
-- ============================================================================

-- Index on month for time-based queries
CREATE INDEX idx_host_activity_month ON host_activity_reduced (month_start);

-- Index on host for host-specific queries
CREATE INDEX idx_host_activity_host ON host_activity_reduced (host);

-- Composite index for common query patterns
CREATE INDEX idx_host_activity_month_host ON host_activity_reduced (month_start, host);

-- GIN index for array operations (optional, for advanced array queries)
-- CREATE INDEX idx_host_hit_array ON host_activity_reduced USING gin (hit_array);
-- CREATE INDEX idx_host_visitors_array ON host_activity_reduced USING gin (unique_visitors_array);

-- ============================================================================
-- Table comments for documentation
-- ============================================================================

COMMENT ON TABLE host_activity_reduced IS 'Monthly aggregated host activity metrics with daily arrays';
COMMENT ON COLUMN host_activity_reduced.month_start IS 'First day of the month (YYYY-MM-01)';
COMMENT ON COLUMN host_activity_reduced.host IS 'Host name from events table';
COMMENT ON COLUMN host_activity_reduced.hit_array IS 'Array of daily hit counts: [day1, day2, ..., day31]. NULL for non-existent days.';
COMMENT ON COLUMN host_activity_reduced.unique_visitors_array IS 'Array of daily unique visitor counts: [day1, day2, ..., day31]. NULL for non-existent days.';
COMMENT ON COLUMN host_activity_reduced.last_updated IS 'Timestamp when record was last updated';

-- ============================================================================
-- Sample data structure (for reference)
-- ============================================================================

/*
Expected data structure:

month_start | host           | hit_array                           | unique_visitors_array
2023-01-01  | google.com     | [245, 389, 156, NULL, 278, ...]    | [45, 67, 23, NULL, 89, ...]
2023-01-01  | facebook.com   | [1240, 890, 1156, 987, ...]       | [234, 189, 267, 198, ...]
2023-01-01  | amazon.com     | [567, NULL, 234, 890, ...]        | [123, NULL, 45, 167, ...]

Where:
- hit_array[1] = hits on January 1st
- hit_array[2] = hits on January 2nd
- unique_visitors_array[1] = unique visitors on January 1st
- NULL values = no activity on that day
*/

-- ============================================================================
-- Verification queries (run after data population)
-- ============================================================================

-- Check table structure
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'host_activity_reduced'
ORDER BY ordinal_position;

-- Sample query to test array operations
/*
-- Total hits for each host in January 2023
SELECT
    host,
    month_start,
    (SELECT SUM(unnested_value) FROM unnest(hit_array) AS unnested_value) as total_monthly_hits,
    (SELECT MAX(unnested_value) FROM unnest(hit_array) AS unnested_value) as peak_daily_hits,
    array_length(hit_array, 1) as days_in_month
FROM host_activity_reduced
WHERE month_start = '2023-01-01'
ORDER BY total_monthly_hits DESC;
*/