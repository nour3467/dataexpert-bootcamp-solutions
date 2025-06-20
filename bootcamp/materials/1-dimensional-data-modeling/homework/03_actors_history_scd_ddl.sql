-- ============================================================================
-- 03_actors_history_scd_ddl.sql
-- DDL for actors_history_scd table - Type 2 SCD implementation
-- ============================================================================

-- TODO: Design the SCD Type 2 table structure
-- Requirements:
-- 1. Track quality_class and is_active changes over time
-- 2. Include start_date and end_date for validity periods
-- 3. One record per actor per change period

DROP TABLE IF EXISTS actors_history_scd CASCADE;

CREATE TABLE actors_history_scd (
    -- TODO: Add actor identification fields
    -- Primary key fields
    actor VARCHAR(255),              -- Actor name (e.g., 'Tom Hanks')
    actorid VARCHAR(50),             -- Unique actor identifier

    -- TODO: Add tracked attributes (what changes we want to capture)
    is_active BOOLEAN,
    quality_class VARCHAR(20),


    -- TODO: Add SCD Type 2 fields (start_date, end_date, current_flag?)
    start_date DATE,        -- When this record became valid
    end_date DATE,          -- When this record ended (NULL = current)
    current_flag BOOLEAN,   -- TRUE = latest record, FALSE = historical

    -- TODO: Add primary key constraint
    PRIMARY KEY (actorid, start_date)
);

-- Appropriate indexes for SCD queries
CREATE INDEX idx_actors_history_actorid ON actors_history_scd(actorid);
CREATE INDEX idx_actors_history_dates ON actors_history_scd(start_date, end_date);
CREATE INDEX idx_actors_history_current ON actors_history_scd(current_flag);
CREATE INDEX idx_actors_history_quality ON actors_history_scd(quality_class);
CREATE INDEX idx_actors_history_active ON actors_history_scd(is_active);



