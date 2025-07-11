-- =============================================
-- Window Functions Analysis for Actor Performance
-- Advanced analytics using window functions
-- =============================================

-- =============================================
-- Analysis 1: Most high-rated films in a 5-film stretch
-- (Adapted from "most games won in 90-game stretch")
-- =============================================

WITH actor_films_ordered AS (
    SELECT
        actor,
        actorid,
        film,
        year,
        rating,
        votes,
        -- Create a film sequence number for each actor (ordered by year, then by votes for tiebreaking)
        ROW_NUMBER() OVER (
            PARTITION BY actorid
            ORDER BY year, votes DESC
        ) AS film_sequence,

        -- Mark high-quality films (rating >= 7.0)
        CASE WHEN rating >= 7.0 THEN 1 ELSE 0 END AS is_high_quality
    FROM actor_films
    WHERE rating IS NOT NULL
      AND year IS NOT NULL
),

rolling_quality_performance AS (
    SELECT
        actor,
        actorid,
        film,
        year,
        rating,
        film_sequence,
        is_high_quality,

        -- Calculate rolling sum of high-quality films in 5-film windows
        SUM(is_high_quality) OVER (
            PARTITION BY actorid
            ORDER BY film_sequence
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS high_quality_in_5_film_stretch,

        -- Calculate average rating in 5-film windows
        ROUND(AVG(rating) OVER (
            PARTITION BY actorid
            ORDER BY film_sequence
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS avg_rating_5_film_stretch,

        -- Count total films in window (should be 5 or less for early career)
        COUNT(*) OVER (
            PARTITION BY actorid
            ORDER BY film_sequence
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS films_in_window
    FROM actor_films_ordered
),

best_5_film_stretches AS (
    SELECT
        actor,
        actorid,
        high_quality_in_5_film_stretch,
        avg_rating_5_film_stretch,
        films_in_window,
        -- Get the best stretch for each actor
        ROW_NUMBER() OVER (
            PARTITION BY actorid
            ORDER BY high_quality_in_5_film_stretch DESC, avg_rating_5_film_stretch DESC
        ) AS stretch_rank
    FROM rolling_quality_performance
    WHERE films_in_window = 5  -- Only consider complete 5-film windows
)

SELECT
    actor,
    high_quality_in_5_film_stretch AS max_quality_films_in_5_stretch,
    avg_rating_5_film_stretch AS avg_rating_best_stretch,
    RANK() OVER (ORDER BY high_quality_in_5_film_stretch DESC, avg_rating_5_film_stretch DESC) AS overall_rank
FROM best_5_film_stretches
WHERE stretch_rank = 1  -- Best stretch for each actor
  AND high_quality_in_5_film_stretch >= 3  -- At least 3 high-quality films
ORDER BY high_quality_in_5_film_stretch DESC, avg_rating_5_film_stretch DESC
LIMIT 20;

-- =============================================
-- Analysis 2: Consecutive high-rated film streaks by actor
-- (Adapted from "games in a row scoring over 10 points")
-- =============================================

WITH actor_film_streaks AS (
    SELECT
        actor,
        actorid,
        film,
        year,
        rating,
        votes,

        -- Order films chronologically for each actor
        ROW_NUMBER() OVER (
            PARTITION BY actorid
            ORDER BY year, votes DESC
        ) AS film_number,

        -- Mark high-quality films (rating >= 7.0)
        CASE WHEN rating >= 7.0 THEN 1 ELSE 0 END AS is_high_quality,

        -- Create groups for consecutive streaks of high-quality films
        ROW_NUMBER() OVER (
            PARTITION BY actorid
            ORDER BY year, votes DESC
        ) - ROW_NUMBER() OVER (
            PARTITION BY actorid, CASE WHEN rating >= 7.0 THEN 1 ELSE 0 END
            ORDER BY year, votes DESC
        ) AS streak_group
    FROM actor_films
    WHERE rating IS NOT NULL
      AND year IS NOT NULL
),

consecutive_quality_streaks AS (
    SELECT
        actor,
        actorid,
        streak_group,
        is_high_quality,
        COUNT(*) AS streak_length,
        MIN(year) AS streak_start_year,
        MAX(year) AS streak_end_year,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating_in_streak,
        STRING_AGG(film, ' | ' ORDER BY year) AS films_in_streak
    FROM actor_film_streaks
    WHERE is_high_quality = 1  -- Only count high-quality film streaks
    GROUP BY actor, actorid, streak_group, is_high_quality
    HAVING COUNT(*) >= 2  -- At least 2 consecutive films
),

longest_streaks_per_actor AS (
    SELECT
        actor,
        actorid,
        MAX(streak_length) AS longest_streak,
        -- Get details of the longest streak
        (ARRAY_AGG(
            streak_start_year ORDER BY streak_length DESC
        ))[1] AS best_streak_start_year,
        (ARRAY_AGG(
            streak_end_year ORDER BY streak_length DESC
        ))[1] AS best_streak_end_year,
        (ARRAY_AGG(
            avg_rating_in_streak ORDER BY streak_length DESC
        ))[1] AS best_streak_avg_rating
    FROM consecutive_quality_streaks
    GROUP BY actor, actorid
)

SELECT
    actor,
    longest_streak AS consecutive_high_quality_films,
    best_streak_start_year,
    best_streak_end_year,
    best_streak_avg_rating,
    RANK() OVER (ORDER BY longest_streak DESC, best_streak_avg_rating DESC) AS streak_rank
FROM longest_streaks_per_actor
ORDER BY longest_streak DESC, best_streak_avg_rating DESC
LIMIT 15;

-- =============================================
-- Analysis 3: Actor performance trends over time
-- Using advanced window functions
-- =============================================

WITH actor_yearly_stats AS (
    SELECT
        actor,
        actorid,
        year,
        COUNT(*) AS films_that_year,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating_that_year,
        SUM(votes) AS total_votes_that_year,

        -- Career progression metrics
        ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY year) AS career_year_number,

        -- Compare to previous year
        LAG(COUNT(*)) OVER (PARTITION BY actorid ORDER BY year) AS films_previous_year,
        LAG(ROUND(AVG(rating)::numeric, 2)) OVER (PARTITION BY actorid ORDER BY year) AS rating_previous_year,

        -- Rolling 3-year averages
        ROUND(AVG(AVG(rating)) OVER (
            PARTITION BY actorid
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS rolling_3yr_avg_rating,

        ROUND(AVG(COUNT(*)) OVER (
            PARTITION BY actorid
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 1) AS rolling_3yr_avg_films_per_year

    FROM actor_films
    WHERE rating IS NOT NULL AND year IS NOT NULL
    GROUP BY actor, actorid, year
),

performance_trends AS (
    SELECT
        actor,
        year,
        films_that_year,
        avg_rating_that_year,
        career_year_number,

        -- Calculate year-over-year changes
        films_that_year - COALESCE(films_previous_year, 0) AS film_count_change,
        avg_rating_that_year - COALESCE(rating_previous_year, 0) AS rating_change,

        rolling_3yr_avg_rating,
        rolling_3yr_avg_films_per_year,

        -- Percentile rankings within each year
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY avg_rating_that_year) AS rating_percentile_that_year,
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY films_that_year) AS productivity_percentile_that_year

    FROM actor_yearly_stats
    WHERE career_year_number >= 2  -- Exclude first year (no comparison possible)
)

SELECT
    actor,
    year,
    films_that_year,
    avg_rating_that_year,
    career_year_number,

    CASE
        WHEN film_count_change > 0 THEN '↗ More Active'
        WHEN film_count_change < 0 THEN '↘ Less Active'
        ELSE '→ Same Activity'
    END AS activity_trend,

    CASE
        WHEN rating_change >= 0.5 THEN '↗ Much Better'
        WHEN rating_change > 0 THEN '↗ Better'
        WHEN rating_change >= -0.5 THEN '→ Similar'
        ELSE '↘ Worse'
    END AS quality_trend,

    rolling_3yr_avg_rating,
    ROUND((rating_percentile_that_year * 100)::numeric, 1) AS rating_percentile,
    ROUND((productivity_percentile_that_year * 100)::numeric, 1) AS productivity_percentile

FROM performance_trends
WHERE rating_percentile_that_year >= 0.8  -- Top 20% performers only
   OR productivity_percentile_that_year >= 0.8  -- Top 20% most productive
ORDER BY year DESC, avg_rating_that_year DESC
LIMIT 25;