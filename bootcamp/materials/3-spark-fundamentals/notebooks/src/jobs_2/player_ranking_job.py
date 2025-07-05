from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, avg, count, desc
from pyspark.sql.window import Window


def do_player_ranking_transformation(spark, match_details_df):
    """
    PostgreSQL equivalent:
    SELECT
        player_gamertag,
        AVG(player_total_kills) as avg_kills,
        AVG(player_total_deaths) as avg_deaths,
        COUNT(*) as games_played,
        RANK() OVER (ORDER BY AVG(player_total_kills) DESC) as kill_rank,
        CASE
            WHEN AVG(player_total_kills) > 20 THEN 'Elite'
            WHEN AVG(player_total_kills) > 10 THEN 'Good'
            ELSE 'Beginner'
        END as skill_tier
    FROM match_details
    WHERE player_total_kills IS NOT NULL
    GROUP BY player_gamertag
    HAVING COUNT(*) >= 5
    ORDER BY avg_kills DESC
    """

    query = """
            SELECT player_gamertag, \
                   AVG(player_total_kills)  as avg_kills, \
                   AVG(player_total_deaths) as avg_deaths, \
                   COUNT(*)                 as games_played, \
                   RANK()                      OVER (ORDER BY AVG(player_total_kills) DESC) as kill_rank, CASE \
                                                                                                              WHEN AVG(player_total_kills) > 20 \
                                                                                                                  THEN 'Elite' \
                                                                                                              WHEN AVG(player_total_kills) > 10 \
                                                                                                                  THEN 'Good' \
                                                                                                              ELSE 'Beginner' \
                END as skill_tier
            FROM player_matches
            WHERE player_total_kills IS NOT NULL
            GROUP BY player_gamertag
            HAVING COUNT(*) >= 5
            ORDER BY avg_kills DESC \
            """

    match_details_df.createOrReplaceTempView("player_matches")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("player_ranking") \
        .getOrCreate()

    # For production, would read from actual match_details table
    match_details_df = spark.table("match_details")
    output_df = do_player_ranking_transformation(spark, match_details_df)
    output_df.write.mode("overwrite").saveAsTable("player_rankings")

    spark.stop()


if __name__ == "__main__":
    main()