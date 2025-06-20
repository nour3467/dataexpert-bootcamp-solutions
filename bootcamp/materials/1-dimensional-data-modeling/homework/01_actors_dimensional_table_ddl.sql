-- ============================================================================
-- 01_actors_dimensional_table_ddl.sql
-- DDL for ACTORS dimensional table
-- ============================================================================

-- Drop table if exists to handle re-runs
DROP TABLE IF EXISTS actors CASCADE;


CREATE TABLE actors (
    -- Primary key fields
    actor VARCHAR(255),              -- Actor name (e.g., 'Tom Hanks')
    actorid VARCHAR(50),             -- Unique actor identifier
    year INTEGER,                    -- Year for this actor's data snapshot

    -- Complex data type: JSONB array of film records
    -- Each film is a JSON object containing film details
    films JSONB,                     -- Array of film objects
                                    -- Format: [{"film": "Movie Name", "votes": 1000, "rating": 8.5, "filmid": "tt123"}]

    -- Derived/calculated fields
    quality_class VARCHAR(20),       -- Actor's performance category
                                    -- Values: 'star', 'good', 'average', 'bad'
                                    -- Based on avg rating of current year films

    is_active BOOLEAN,              -- TRUE if an actor made films this year
                                   -- FALSE if no films this year

    -- Primary key constraint
    PRIMARY KEY (actorid, year)
);

-- ============================================================================
-- POSTGRES-SPECIFIC OPTIMIZATIONS:
-- ============================================================================

-- Create indexes for better query performance
CREATE INDEX idx_actors_year ON actors(year);
CREATE INDEX idx_actors_quality_class ON actors(quality_class);
CREATE INDEX idx_actors_is_active ON actors(is_active);

-- GIN index for JSONB queries (enables fast film searches)
CREATE INDEX idx_actors_films_gin ON actors USING GIN(films);

