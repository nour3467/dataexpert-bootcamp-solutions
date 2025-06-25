-- ============================================================================
-- 01_deduplicate_game_details.sql
-- Deduplication query for game_details table
-- ============================================================================



WITH game_details_deduped AS (
    SELECT  *,
            ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS rn
    FROM game_details
)

SELECT *
FROM game_details_deduped
WHERE rn=1;



