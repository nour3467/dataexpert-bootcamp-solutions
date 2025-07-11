-- =============================================
-- GROUPING SETS Analysis for Actor Performance
-- Efficient aggregations across multiple dimensions
-- =============================================

-- First, create the film quality tiers in a CTE
WITH actor_films_with_tiers AS (
    SELECT
        actor,
        year,
        rating,
        votes,
        CASE
            WHEN rating >= 8.0 THEN 'Excellent (8.0+)'
            WHEN rating >= 7.0 THEN 'Good (7.0-7.9)'
            WHEN rating >= 6.0 THEN 'Average (6.0-6.9)'
            WHEN rating >= 5.0 THEN 'Below Average (5.0-5.9)'
            ELSE 'Poor (<5.0)'
        END AS film_quality_tier
    FROM actor_films
    WHERE rating IS NOT NULL
      AND votes IS NOT NULL
      AND year IS NOT NULL
)

SELECT
    -- Dimensions for grouping
    actor,
    year,
    film_quality_tier,

    -- Aggregated metrics
    COUNT(*) AS total_films,
    ROUND(AVG(rating)::numeric, 2) AS avg_rating,
    SUM(votes) AS total_votes,
    ROUND(AVG(votes)::numeric, 0) AS avg_votes_per_film,
    MAX(rating) AS highest_rated_film_rating,
    MIN(rating) AS lowest_rated_film_rating,

    -- Identify which grouping set this row belongs to
    CASE
        WHEN GROUPING(actor) = 0 AND GROUPING(year) = 0 AND GROUPING(film_quality_tier) = 0
        THEN 'Actor + Year + Quality Tier'
        WHEN GROUPING(actor) = 0 AND GROUPING(year) = 0 AND GROUPING(film_quality_tier) = 1
        THEN 'Actor + Year (All Qualities)'
        WHEN GROUPING(actor) = 0 AND GROUPING(year) = 1 AND GROUPING(film_quality_tier) = 0
        THEN 'Actor + Quality Tier (All Years)'
        WHEN GROUPING(actor) = 1 AND GROUPING(year) = 0 AND GROUPING(film_quality_tier) = 0
        THEN 'Year + Quality Tier (All Actors)'
        WHEN GROUPING(actor) = 0 AND GROUPING(year) = 1 AND GROUPING(film_quality_tier) = 1
        THEN 'Actor Only (Career Total)'
        WHEN GROUPING(actor) = 1 AND GROUPING(year) = 0 AND GROUPING(film_quality_tier) = 1
        THEN 'Year Only (Industry Total)'
        WHEN GROUPING(actor) = 1 AND GROUPING(year) = 1 AND GROUPING(film_quality_tier) = 0
        THEN 'Quality Tier Only (All Time)'
        ELSE 'Grand Total'
    END AS analysis_level

FROM actor_films_with_tiers

GROUP BY GROUPING SETS (
    -- Detailed breakdown: Actor performance by year and film quality
    (actor, year, film_quality_tier),

    -- Actor performance by year (all film qualities combined)
    (actor, year),

    -- Actor performance by film quality (all years combined)
    (actor, film_quality_tier),

    -- Industry trends by year and quality (all actors combined)
    (year, film_quality_tier),

    -- Individual actor career totals
    (actor),

    -- Industry totals by year
    (year),

    -- Industry totals by film quality tier
    (film_quality_tier),

    -- Grand total across all dimensions
    ()
)

ORDER BY
    analysis_level,
    total_films DESC,
    avg_rating DESC;

-- =============================================
-- Specialized Queries Using the GROUPING SETS Results
-- =============================================

-- Question 1: Which actor has the highest average rating in a single year?
WITH actor_year_performance AS (
    SELECT
        actor,
        year,
        COUNT(*) AS total_films,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating,
        SUM(votes) AS total_votes
    FROM actor_films
    WHERE rating IS NOT NULL AND year IS NOT NULL
    GROUP BY actor, year
    HAVING COUNT(*) >= 2  -- At least 2 films to qualify
)
SELECT
    actor,
    year,
    total_films,
    avg_rating,
    total_votes,
    RANK() OVER (ORDER BY avg_rating DESC, total_films DESC) AS ranking
FROM actor_year_performance
ORDER BY avg_rating DESC, total_films DESC
LIMIT 10;

-- Question 2: Which actor has been most prolific in high-quality films?
WITH high_quality_performance AS (
    SELECT
        actor,
        COUNT(*) AS excellent_films,
        ROUND(AVG(rating)::numeric, 2) AS avg_excellent_rating,
        SUM(votes) AS total_votes_excellent
    FROM actor_films
    WHERE rating >= 8.0  -- Excellent films only
    GROUP BY actor
    HAVING COUNT(*) >= 3  -- At least 3 excellent films
)
SELECT
    actor,
    excellent_films,
    avg_excellent_rating,
    total_votes_excellent,
    RANK() OVER (ORDER BY excellent_films DESC, avg_excellent_rating DESC) AS ranking
FROM high_quality_performance
ORDER BY excellent_films DESC, avg_excellent_rating DESC
LIMIT 10;

-- Question 3: What year had the most high-quality films released?
WITH yearly_quality AS (
    SELECT
        year,
        COUNT(*) AS total_films,
        COUNT(CASE WHEN rating >= 8.0 THEN 1 END) AS excellent_films,
        COUNT(CASE WHEN rating >= 7.0 THEN 1 END) AS good_or_better_films,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating,
        ROUND((COUNT(CASE WHEN rating >= 8.0 THEN 1 END) * 100.0 / COUNT(*))::numeric, 2) AS pct_excellent
    FROM actor_films
    WHERE rating IS NOT NULL AND year IS NOT NULL
    GROUP BY year
    HAVING COUNT(*) >= 10  -- Years with at least 10 films
)
SELECT
    year,
    total_films,
    excellent_films,
    good_or_better_films,
    avg_rating,
    pct_excellent,
    RANK() OVER (ORDER BY excellent_films DESC) AS rank_by_excellent_count,
    RANK() OVER (ORDER BY pct_excellent DESC) AS rank_by_excellent_percentage
FROM yearly_quality
ORDER BY excellent_films DESC
LIMIT 15;