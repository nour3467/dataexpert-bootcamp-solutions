import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_kafka_source(t_env):
    """Create Kafka source table for reading events"""
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events_source"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = 'session-homework-group',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_results_sink(t_env):
    """Create PostgreSQL sink for session analysis results"""
    table_name = 'user_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name



def session_analysis():
    """Main function to run session analysis"""
    # Set up Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)  # 10 seconds
    env.set_parallelism(2)

    # Set up table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source and sink tables
        source_table = create_kafka_source(t_env)
        session_sink = create_session_results_sink(t_env)

        print("Starting session analysis...")

        # Use Table API for session windows
        # Create the session analysis using Table API
        result_table = t_env.from_path(source_table) \
            .window(Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("s")) \
            .group_by(col("s"), col("ip"), col("host")) \
            .select(
                col("s").start.alias("session_start"),
                col("s").end.alias("session_end"),
                col("ip"),
                col("host"),
                col("ip").count.alias("num_events")
            )

        # Insert results
        result_table.execute_insert(session_sink)

        # Execute the session analysis
        print("Executing session analysis...")
        result_table.execute_insert(session_sink).wait()

    except Exception as e:
        print(f"Session analysis failed: {str(e)}")


if __name__ == '__main__':
    session_analysis()
