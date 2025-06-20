-- ============================================================================
-- 05_actors_history_scd_incremental.sql
-- Incremental update query for actors_history_scd table
-- ============================================================================

-- SCENARIO: We already have SCD data loaded up to 2020
--           Now we want to process new 2021 data from actors table

-- Sub-Task 5.1: Get new year's data
-- TODO: Extract actor data for the new year (2021) from actors table
-- Expected: New year's quality_class and is_active for each actor

-- YOUR QUERY HERE:
SELECT
    actor,
    actorid,
    year,
    quality_class,
    is_active
FROM actors
WHERE year = 2019;



-- ============================================================================
-- Sub-Task 5.2: Get current active SCD records
-- TODO: Get all current records (current_flag = TRUE) from actors_history_scd
-- Expected: Current state of each actor before new year

-- YOUR QUERY HERE:
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date
FROM actors_history_scd
WHERE current_flag = TRUE;



-- ============================================================================
-- Sub-Task 5.3: Compare new vs current to detect changes
-- TODO: Join new year data with current SCD records to identify changes
-- Hint: LEFT JOIN to handle both existing and new actors

-- YOUR QUERY HERE:
SELECT
    -- New year data
    new_data.actor,
    new_data.actorid,
    new_data.quality_class AS new_quality,
    new_data.is_active AS new_active,

    -- Current SCD data
    current_scd.quality_class AS current_quality,
    current_scd.is_active AS current_active,

    -- Detect what changed
    CASE
        WHEN current_scd.actorid IS NULL THEN 'NEW_ACTOR'
        WHEN new_data.quality_class != current_scd.quality_class THEN 'QUALITY_CHANGED'
        WHEN new_data.is_active != current_scd.is_active THEN 'ACTIVITY_CHANGED'
        ELSE 'NO_CHANGE'
    END AS change_type

FROM actors new_data  -- 2021 data
LEFT JOIN actors_history_scd current_scd
    ON new_data.actorid = current_scd.actorid
    AND current_scd.current_flag = TRUE  -- Only current records
WHERE new_data.year = 2021;




-- ============================================================================
-- Sub-Task 5.4: Close out changed records (UPDATE)
-- TODO: Update existing records that changed - set end_date and current_flag = FALSE
-- Hint: UPDATE actors_history_scd SET end_date = ?, current_flag = FALSE WHERE ...

-- YOUR QUERY HERE:
UPDATE actors_history_scd
SET
    end_date = '2020-12-31',
    current_flag = FALSE
WHERE current_flag = TRUE
  AND actorid IN (
      SELECT new_data.actorid
      FROM actors new_data
      LEFT JOIN actors_history_scd current_scd
          ON new_data.actorid = current_scd.actorid
          AND current_scd.current_flag = TRUE
      WHERE new_data.year = 2021
        AND (
            new_data.quality_class != current_scd.quality_class
            OR new_data.is_active != current_scd.is_active
        )
  );



-- ============================================================================
-- Sub-Task 5.5: Insert new records for changes
-- TODO: Insert new SCD records for actors whose data changed
-- Plus insert records for brand new actors

-- YOUR QUERY HERE:
INSERT INTO actors_history_scd (actor, actorid, quality_class, is_active, start_date, end_date, current_flag)
SELECT
    new_data.actor,
    new_data.actorid,
    new_data.quality_class,
    new_data.is_active,
    '2021-01-01' AS start_date,
    NULL AS end_date,
    TRUE AS current_flag
FROM actors new_data
LEFT JOIN actors_history_scd current_scd
    ON new_data.actorid = current_scd.actorid
    AND current_scd.current_flag = TRUE
WHERE new_data.year = 2021
  AND (
      -- New actor
      current_scd.actorid IS NULL
      OR
      -- Existing actor with changes
      new_data.quality_class != current_scd.quality_class
      OR
      new_data.is_active != current_scd.is_active
  );




-- ============================================================================
-- FINAL COMPLETE INCREMENTAL SOLUTION
-- TODO: Combine all sub-tasks into a complete incremental update process
-- This should be a transaction that:
-- 1. Updates existing records that changed
-- 2. Inserts new records for changes and new actors
-- ============================================================================

-- YOUR COMPLETE INCREMENTAL QUERY HERE:
-- YOUR COMPLETE INCREMENTAL QUERY HERE:
BEGIN;

-- Step 1: Close out records that changed
UPDATE actors_history_scd
SET
    end_date = '2020-12-31',
    current_flag = FALSE
WHERE current_flag = TRUE
  AND actorid IN (
      SELECT new_data.actorid
      FROM actors new_data
      LEFT JOIN actors_history_scd current_scd
          ON new_data.actorid = current_scd.actorid
          AND current_scd.current_flag = TRUE
      WHERE new_data.year = 2021
        AND (
            new_data.quality_class != current_scd.quality_class
            OR new_data.is_active != current_scd.is_active
        )
  );

-- Step 2: Insert new records for changes and new actors
INSERT INTO actors_history_scd (actor, actorid, quality_class, is_active, start_date, end_date, current_flag)
SELECT
    new_data.actor,
    new_data.actorid,
    new_data.quality_class,
    new_data.is_active,
    '2021-01-01' AS start_date,
    NULL AS end_date,
    TRUE AS current_flag
FROM actors new_data
LEFT JOIN actors_history_scd current_scd
    ON new_data.actorid = current_scd.actorid
    AND current_scd.current_flag = TRUE
WHERE new_data.year = 2021
  AND (
      -- New actor
      current_scd.actorid IS NULL
      OR
      -- Existing actor with changes
      new_data.quality_class != current_scd.quality_class
      OR
      new_data.is_active != current_scd.is_active
  );

COMMIT;