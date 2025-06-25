-- ============================================================================
-- 02_user_devices_cumulated_ddl.sql
-- DDL for user_devices_cumulated table
-- ============================================================================

-- Drop table if exists
DROP TABLE IF EXISTS user_devices_cumulated CASCADE;

CREATE TABLE user_devices_cumulated (
    user_id TEXT,              -- Which user (BIGINT out of range )
    browser_type TEXT,           -- Which browser (Chrome, Firefox, etc.)
    device_activity_datelist DATE[],  -- ALL dates this user was active with this browser
    date DATE,                   -- Snapshot date (when this record was created)
    PRIMARY KEY (user_id, browser_type, date)
);


