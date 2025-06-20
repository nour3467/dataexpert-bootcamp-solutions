-- ============================================================================
-- 02_actors_cumulative_population_query.sql
-- Populates actors table one year at a time from actor_films
-- ============================================================================

-- Sub-Task 2.1: Basic Year Filtering & Grouping
-- TODO: Write query that groups actor_films by actor for year 2020
-- Expected: Each actor appears once with basic info (actor, actorid, year)

-- YOUR QUERY HERE:
SELECT actor, actorid, year
FROM actor_films
WHERE year = 2020
GROUP BY actor, actorid, year
ORDER BY actor, actorid, year;




-- ============================================================================
-- Sub-Task 2.2: Aggregate films into JSONB array
-- TODO: Create JSON array of films for each actor
-- Hint: Use json_agg() function with json_build_object()

-- YOUR QUERY HERE:
SELECT actor, actorid, year,
        json_agg(json_build_object(
                'film', film,
                'votes', votes,
                'rating', rating,
                'filmid', filmid
                 )
        ) AS films
FROM actor_films
WHERE year = 2020
GROUP BY actor, actorid, year
ORDER BY actor, actorid, year;




-- ============================================================================
-- Sub-Task 2.3: Calculate quality_class from average rating
-- TODO: Calculate avg(rating) and apply business rules
-- star: >8, good: >7 and ≤8, average: >6 and ≤7, bad: ≤6

-- YOUR QUERY HERE:
SELECT actor, actorid, year,
       CASE WHEN AVG(rating)>8 THEN 'star'
            WHEN AVG(rating)>7 THEN 'good'
            WHEN AVG(rating)>6 THEN 'average'
            WHEN AVG(rating)>5 THEN 'bad'
            ELSE 'none'
       END AS quality_class
FROM actor_films
WHERE year = 2020
GROUP BY actor, actorid, year
ORDER BY actor, actorid, year;



-- ============================================================================
-- Sub-Task 2.4: Determine is_active flag
-- TODO: is_active = TRUE (since we're processing films from this year)

-- YOUR QUERY HERE:
SELECT actor, actorid, year,
       TRUE AS is_active
FROM actor_films
WHERE year = 2020
GROUP BY actor, actorid, year
ORDER BY actor, actorid, year;




-- ============================================================================
-- FINAL COMPLETE SOLUTION - Insert into actors table
-- ============================================================================

INSERT INTO actors (actor, actorid, year, films, quality_class, is_active)
-- TODO: Combine all sub-tasks into one complete query
-- This should transform actor_films for a specific year into actors format

-- YOUR COMPLETE QUERY HERE:
SELECT
    actor,
    actorid,
    year,
    json_agg(json_build_object(
        'film', film,
        'votes', votes,
        'rating', rating,
        'filmid', filmid
    )) AS films,
    CASE
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating) > 7 THEN 'good'
        WHEN AVG(rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    CASE
        WHEN COUNT(*) > 0 THEN TRUE
        ELSE FALSE
    END AS is_active
FROM actor_films
WHERE year = 2019
GROUP BY actor, actorid, year
ON CONFLICT (actorid, year) DO NOTHING;





-- ============================================================================
-- USAGE: To populate actors table for specific year
-- EXAMPLE: Change 2020 to any year you want to process
-- ============================================================================