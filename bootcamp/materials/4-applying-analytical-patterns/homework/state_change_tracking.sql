-- =============================================
-- State Change Tracking for Actors
-- Tracks actor career status changes over time
-- =============================================

WITH actor_status_changes AS (
    SELECT
        actor,
        actorid,
        year,
        is_active,
        -- Get previous year's status for each actor
        LAG(is_active) OVER (
            PARTITION BY actorid
            ORDER BY year
        ) AS previous_year_active,
        -- Get the first year we see this actor
        MIN(year) OVER (PARTITION BY actorid) AS first_appearance_year
    FROM actors
    WHERE is_active IS NOT NULL
),

actor_state_tracking AS (
    SELECT
        actor,
        actorid,
        year,
        is_active,
        previous_year_active,
        first_appearance_year,

        -- Determine the state change based on current and previous status
        CASE
            -- New actor entering the industry
            WHEN year = first_appearance_year AND is_active = true
            THEN 'New Actor'

            -- Actor retiring (was active, now inactive)
            WHEN previous_year_active = true AND is_active = false
            THEN 'Retired'

            -- Actor continuing their career (was active, still active)
            WHEN previous_year_active = true AND is_active = true
            THEN 'Continued Acting'

            -- Actor returning from retirement (was inactive, now active)
            WHEN previous_year_active = false AND is_active = true
            THEN 'Returned from Retirement'

            -- Actor staying retired (was inactive, still inactive)
            WHEN previous_year_active = false AND is_active = false
            THEN 'Stayed Retired'

            -- First time inactive (new actor who immediately went inactive)
            WHEN year = first_appearance_year AND is_active = false
            THEN 'New Actor (Inactive)'

            ELSE 'Unknown Status'
        END AS career_status_change
    FROM actor_status_changes
)

SELECT
    actor,
    actorid,
    year,
    CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END AS current_status,
    career_status_change,
    -- Add some context about their career
    year - first_appearance_year AS years_since_debut
FROM actor_state_tracking
WHERE career_status_change != 'Unknown Status'
ORDER BY actor, year;

-- =============================================
-- Summary Statistics by Career Status
-- =============================================

WITH actor_status_changes AS (
    SELECT
        actor,
        actorid,
        year,
        is_active,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS previous_year_active,
        MIN(year) OVER (PARTITION BY actorid) AS first_appearance_year
    FROM actors
    WHERE is_active IS NOT NULL
),

actor_state_tracking AS (
    SELECT
        year,
        CASE
            WHEN year = first_appearance_year AND is_active = true THEN 'New Actor'
            WHEN previous_year_active = true AND is_active = false THEN 'Retired'
            WHEN previous_year_active = true AND is_active = true THEN 'Continued Acting'
            WHEN previous_year_active = false AND is_active = true THEN 'Returned from Retirement'
            WHEN previous_year_active = false AND is_active = false THEN 'Stayed Retired'
            WHEN year = first_appearance_year AND is_active = false THEN 'New Actor (Inactive)'
            ELSE 'Unknown Status'
        END AS career_status_change
    FROM actor_status_changes
)

SELECT
    career_status_change,
    COUNT(*) AS total_occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM actor_state_tracking
WHERE career_status_change != 'Unknown Status'
GROUP BY career_status_change
ORDER BY total_occurrences DESC;