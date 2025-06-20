-- ============================================================================
-- 04_actors_history_scd_backfill.sql
-- Backfill query to populate actors_history_scd table from actors table
-- ============================================================================

-- Goal: Transform actors table (one row per actor per year)
--       into SCD Type 2 format (one row per actor per change period)

-- Sub-Task 4.1: Detect changes using window functions
-- TODO: Use LAG() to compare current vs previous year values
-- Hint: LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year)

-- YOUR QUERY HERE:
SELECT
    actor,
    actorid,
    year,
    quality_class,           -- VARCHAR: 'star', 'good', 'average', 'bad'
    is_active,               -- BOOLEAN: TRUE/FALSE
    LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) AS prev_quality,
    LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS prev_active,
    CASE
        WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE  -- First record
        WHEN quality_class != LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Quality changed
        WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Activity changed
        ELSE FALSE  -- No change
    END AS has_changed       -- BOOLEAN: TRUE if changed, FALSE if same
FROM actors;




-- ============================================================================
-- Sub-Task 4.2: Determine start and end dates
-- TODO: Calculate start_date and end_date for each period
-- Logic: start_date = January 1st of year, end_date = December 31st of year
-- ============================================================================
-- YOUR QUERY HERE:
SELECT
    actor,
    actorid,
    year,
    quality_class,           -- VARCHAR: 'star', 'good', 'average', 'bad'
    is_active,               -- BOOLEAN: TRUE/FALSE

    -- Add date conversions here:
    MAKE_DATE(year, 1, 1) AS start_date,
    MAKE_DATE(year, 12, 31) AS end_date,

    LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) AS prev_quality,
    LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS prev_active,
    CASE
        WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE  -- First record
        WHEN quality_class != LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Quality changed
        WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Activity changed
        ELSE FALSE  -- No change
    END AS has_changed       -- BOOLEAN: TRUE if changed, FALSE if same
FROM actors;




-- Sub-Task 4.3: Handle current records
-- TODO: Set end_date = NULL for the latest record of each actor
-- Hint: Use LEAD() or ROW_NUMBER() to identify last record
-- ============================================================================
-- YOUR QUERY HERE:
SELECT
    actor,
    actorid,
    year,
    quality_class,           -- VARCHAR: 'star', 'good', 'average', 'bad'
    is_active,               -- BOOLEAN: TRUE/FALSE

    -- Add date conversions here:
    MAKE_DATE(year, 1, 1) AS start_date,

    CASE
        WHEN LEAD(year) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN NULL
        ELSE MAKE_DATE(year, 12, 31)
    END AS end_date,

    LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) AS prev_quality,
    LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS prev_active,
    CASE
        WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE  -- First record
        WHEN quality_class != LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Quality changed
        WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Activity changed
        ELSE FALSE  -- No change
    END AS has_changed       -- BOOLEAN: TRUE if changed, FALSE if same
FROM actors;



-- Sub-Task 4.4: Filter only records where changes occurred
-- TODO: Only include records where quality_class or is_active changed
-- Or it's the first record for an actor
-- ============================================================================

-- Sub-Task 4.4: Filter only records where changes occurred
-- YOUR QUERY HERE:
-- Sub-Task 4.4: Filter only records where changes occurred
-- YOUR QUERY HERE:
WITH actor_changes AS (
    SELECT
        actor,
        actorid,
        year,
        quality_class,           -- VARCHAR: 'star', 'good', 'average', 'bad'
        is_active,               -- BOOLEAN: TRUE/FALSE

        -- Add date conversions here:
        MAKE_DATE(year, 1, 1) AS start_date,

        CASE
            WHEN LEAD(year) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN NULL
            ELSE MAKE_DATE(year, 12, 31)
        END AS end_date,

        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) AS prev_quality,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS prev_active,
        CASE
            WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE  -- First record
            WHEN quality_class != LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Quality changed
            WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Activity changed
            ELSE FALSE  -- No change
        END AS has_changed       -- BOOLEAN: TRUE if changed, FALSE if same
    FROM actors
)
SELECT
    actor,
    actorid,
    year,
    quality_class,
    is_active,
    start_date,
    end_date,
    prev_quality,
    prev_active,
    has_changed
FROM actor_changes
WHERE has_changed = TRUE;

-- ============================================================================
-- FINAL COMPLETE BACKFILL QUERY
-- ============================================================================

INSERT INTO actors_history_scd (actor, actorid, quality_class, is_active, start_date, end_date, current_flag)
-- TODO: Write complete query that:
-- 1. Detects changes in quality_class and is_active
-- 2. Creates SCD Type 2 records with proper date ranges
-- 3. Sets current_flag appropriately

-- YOUR COMPLETE BACKFILL QUERY HERE:
INSERT INTO actors_history_scd (actor, actorid, quality_class, is_active, start_date, end_date, current_flag)
-- TODO: Write complete query that:
-- 1. Detects changes in quality_class and is_active
-- 2. Creates SCD Type 2 records with proper date ranges
-- 3. Sets current_flag appropriately

-- YOUR COMPLETE BACKFILL QUERY HERE:
WITH actor_changes AS (
    SELECT
        actor,
        actorid,
        year,
        quality_class,
        is_active,
        MAKE_DATE(year, 1, 1) AS start_date,
        CASE
            WHEN LEAD(year) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN NULL
            ELSE MAKE_DATE(year, 12, 31)
        END AS end_date,
        CASE
            WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE  -- First record
            WHEN quality_class != LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Quality changed
            WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) THEN TRUE  -- Activity changed
            ELSE FALSE  -- No change
        END AS has_changed
    FROM actors
),
filtered_changes AS (
    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actor_changes
    WHERE has_changed = TRUE
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date,
    CASE
        WHEN end_date IS NULL THEN TRUE   -- Current record (latest)
        ELSE FALSE                        -- Historical record
    END AS current_flag
FROM filtered_changes
ORDER BY actorid, start_date;




