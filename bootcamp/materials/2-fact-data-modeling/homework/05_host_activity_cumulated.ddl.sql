-- ============================================================================
-- 05_host_activity_cumulated.ddl.sql
-- Table definition for host activity cumulated data
-- Run this ONCE before running the incremental scripts
-- ============================================================================

CREATE TABLE IF NOT EXISTS host_activity_cumulated (
    user_id TEXT NOT NULL,
    host_name TEXT NOT NULL,  -- This will store the 'host' values from events table
    host_activity_datelist DATE[] NOT NULL,  -- Array of dates when user was active on this host
    date DATE NOT NULL,  -- Snapshot date (when this record was created)

    PRIMARY KEY (user_id, host_name, date)
);

-- ============================================================================
-- Indexes for performance
-- ============================================================================

-- Index on date for time-based queries
CREATE INDEX IF NOT EXISTS idx_host_cumulated_date
ON host_activity_cumulated (date);

-- Index on host_name for host-specific analysis
CREATE INDEX IF NOT EXISTS idx_host_cumulated_host
ON host_activity_cumulated (host_name);

-- Index on user_id for user-specific queries
CREATE INDEX IF NOT EXISTS idx_host_cumulated_user
ON host_activity_cumulated (user_id);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_host_cumulated_date_host
ON host_activity_cumulated (date, host_name);

-- GIN index for array operations
CREATE INDEX IF NOT EXISTS idx_host_activity_datelist_gin
ON host_activity_cumulated USING gin (host_activity_datelist);

-- ============================================================================
-- Table comments for documentation
-- ============================================================================

COMMENT ON TABLE host_activity_cumulated IS 'Daily cumulative snapshots of user activity per host';
COMMENT ON COLUMN host_activity_cumulated.user_id IS 'User identifier from events table';
COMMENT ON COLUMN host_activity_cumulated.host_name IS 'Host name from events.host column';
COMMENT ON COLUMN host_activity_cumulated.host_activity_datelist IS 'Cumulative array of dates when user was active on this host';
COMMENT ON COLUMN host_activity_cumulated.date IS 'Snapshot date - when this cumulative record was created';