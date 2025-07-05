from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, date_trunc, collect_list


def do_user_activity_transformation(spark, events_df):
    """
    PostgreSQL equivalent:
    SELECT
        user_id,
        DATE_TRUNC('month', event_time) as month_start,
        COUNT(*) as total_events,
        COUNT(DISTINCT host) as unique_hosts,
        AVG(CASE WHEN url = '/' THEN 1 ELSE 0 END) as homepage_ratio
    FROM events
    WHERE user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('month', event_time)
    ORDER BY total_events DESC
    """

    query = """
            SELECT user_id, \
                   DATE_TRUNC('month', event_time)                as month_start, \
                   COUNT(*)                                       as total_events, \
                   COUNT(DISTINCT host)                           as unique_hosts, \
                   AVG(CASE WHEN url = '/' THEN 1.0 ELSE 0.0 END) as homepage_ratio
            FROM user_events
            WHERE user_id IS NOT NULL
            GROUP BY user_id, DATE_TRUNC('month', event_time)
            ORDER BY total_events DESC \
            """

    events_df.createOrReplaceTempView("user_events")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("user_activity") \
        .getOrCreate()

    # For production, would read from actual events table
    events_df = spark.table("events")
    output_df = do_user_activity_transformation(spark, events_df)
    output_df.write.mode("overwrite").saveAsTable("user_activity_monthly")

    spark.stop()


if __name__ == "__main__":
    main()