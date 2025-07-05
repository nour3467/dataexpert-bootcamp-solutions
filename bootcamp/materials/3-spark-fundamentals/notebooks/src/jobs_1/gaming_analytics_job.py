from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, count, desc, col, max as spark_max, sum as spark_sum


def main():
    # Create Spark session with LOCAL warehouse (Iceberg config optional for this task)
    spark = SparkSession.builder \
        .appName("gaming_analytics") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "file:///home/iceberg/warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()

    # Add this at the beginning of your script
    spark.sparkContext.setLogLevel("ERROR")

    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

    # Load all datasets
    print("Loading datasets...")
    match_details = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "/home/iceberg/data/match_details.csv")
    matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
    medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
    maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
    medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "/home/iceberg/data/medals_matches_players.csv")

    # Create bucketed tables (16 buckets on match_id) - LOCAL storage
    print("Creating bucketed tables...")

    # Drop tables if they exist
    spark.sql("DROP TABLE IF EXISTS bootcamp.match_details_bucketed")
    spark.sql("DROP TABLE IF EXISTS bootcamp.matches_bucketed")
    spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed")

    match_details.write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.match_details_bucketed")

    matches.write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.matches_bucketed")

    medals_matches_players.write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.medals_matches_players_bucketed")

    # Read bucketed tables with aliases to avoid ambiguity
    md = spark.table("bootcamp.match_details_bucketed").alias("md")
    m = spark.table("bootcamp.matches_bucketed").alias("m")
    mmp = spark.table("bootcamp.medals_matches_players_bucketed").alias("mmp")

    # Explicitly broadcast small tables with aliases
    medals_bc = broadcast(medals.alias("medals"))
    maps_bc = broadcast(maps.alias("maps"))

    # Join all data using bucket joins + broadcast joins - FIXED with aliases
    print("Joining data...")

    # First bucket join the main tables
    main_joined = md.join(m, col("md.match_id") == col("m.match_id")) \
        .join(mmp, (col("md.match_id") == col("mmp.match_id")) &
              (col("md.player_gamertag") == col("mmp.player_gamertag")), "left")

    # Then broadcast join the small dimension tables
    full_data = main_joined \
        .join(medals_bc, col("mmp.medal_id") == col("medals.medal_id"), "left") \
        .join(maps_bc, col("m.mapid") == col("maps.mapid"), "left")

    print("Analyzing gaming data...")

    # Question 1: Which player averages the most kills per game?
    top_killer = full_data.groupBy(col("md.player_gamertag")) \
        .agg(avg(col("md.player_total_kills")).alias("avg_kills")) \
        .orderBy(desc("avg_kills")) \
        .limit(1)

    print("Top Killer:")
    top_killer.show()

    # Question 2: Which playlist gets played the most?
    top_playlist = full_data.groupBy(col("m.playlist_id")) \
        .agg(count(col("md.match_id")).alias("match_count")) \
        .orderBy(desc("match_count")) \
        .limit(1)

    print("Most Played Playlist:")
    top_playlist.show()

    # Question 3: Which map gets played the most?
    top_map = full_data.filter(col("maps.name").isNotNull()) \
        .groupBy(col("maps.name")) \
        .agg(count(col("md.match_id")).alias("match_count")) \
        .orderBy(desc("match_count")) \
        .limit(1)

    print("Most Played Map:")
    top_map.show()

    # Question 4: Which map do players get the most Killing Spree medals on? (FIXED)
    killing_spree_by_map = full_data.filter(
        col("maps.name").isNotNull() &
        col("medals.name").isNotNull() &
        col("medals.name").contains("Killing Spree")
    ) \
    .groupBy(col("maps.name")) \
    .agg(spark_sum(col("mmp.count")).alias("total_killing_spree_medals")) \
    .orderBy(desc("total_killing_spree_medals")) \
    .limit(1)

    print("Map with Most Killing Spree Medals:")
    killing_spree_by_map.show()

    # Test sortWithinPartitions for smallest data size (ENHANCED with 3 strategies)
    print("Testing sortWithinPartitions with 3 different strategies...")

    # Strategy 1: Sort by playlist (low cardinality)
    sorted_by_playlist = full_data.repartition(16) \
        .sortWithinPartitions(col("m.playlist_id"))

    # Strategy 2: Sort by map name (low cardinality)
    sorted_by_map = full_data.filter(col("maps.name").isNotNull()) \
        .repartition(16) \
        .sortWithinPartitions(col("maps.name"))

    # Strategy 3: Sort by medal name (low cardinality) - NEW STRATEGY
    sorted_by_medal = full_data.filter(col("medals.name").isNotNull()) \
        .repartition(16) \
        .sortWithinPartitions(col("medals.name"))

    print("Data size comparison:")
    print(f"Original data partitions: {full_data.rdd.getNumPartitions()}")
    print(f"Sorted by playlist partitions: {sorted_by_playlist.rdd.getNumPartitions()}")
    print(f"Sorted by map partitions: {sorted_by_map.rdd.getNumPartitions()}")
    print(f"Sorted by medal partitions: {sorted_by_medal.rdd.getNumPartitions()}")

    # Sample counts to compare data distribution efficiency
    print("\nData distribution analysis:")
    print(f"Playlist strategy sample count: {sorted_by_playlist.limit(100).count()}")
    print(f"Map strategy sample count: {sorted_by_map.limit(100).count()}")
    print(f"Medal strategy sample count: {sorted_by_medal.limit(100).count()}")

    # Show explain plan to verify bucket joins
    print("\nExplain plan for main joined data (bucket joins):")
    main_joined.explain()

    # Additional insight: Show the actual medal names to verify our Killing Spree filter
    print("\nAvailable medal types (for verification):")
    medals.select("name").distinct().orderBy("name").show(20, truncate=False)

    spark.stop()
    print("Job completed!")


if __name__ == "__main__":
    main()