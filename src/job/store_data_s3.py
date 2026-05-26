from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os

TARGET_BUCKET = os.getenv("TARGET_S3_BUCKET")

def create_events_source_kafka(t_env):
    table_name = "clickstream_raw"
    source_ddl = f"""
        CREATE TABLE {table_name}(
            session_id VARCHAR,
            client_id VARCHAR,
            user_id VARCHAR,
            ip_address VARCHAR,
            device_category VARCHAR,
            os_browser VARCHAR,
            utm_source VARCHAR,
            cart_total FLOAT,
            event_id VARCHAR,
            event_timestamp BIGINT,
            event_name VARCHAR,
            page_url VARCHAR,
            product_id VARCHAR,
            category VARCHAR,
            price FLOAT,
            quantity INTEGER,
            transaction_id VARCHAR,
            ts AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK FOR ts AS ts - INTERVAL '20' SECOND
        )
            WITH(
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'topic' = 'ecommerce_clickstream',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'avro-confluent',
            'avro-confluent.schema-registry.url' = 'http://redpanda:8081',
            'avro-confluent.schema-registry.subject' = 'ecommerce_clickstream-value'
            )
        """
    
    t_env.execute_sql(source_ddl)
    return table_name

def create_kafka_dlq_sink(t_env):
    dlq_table = "kafka_dlq"
    sink_ddl = f"""
        CREATE TABLE {dlq_table}(
            session_id VARCHAR,
            client_id VARCHAR,
            user_id VARCHAR,
            ip_address VARCHAR,
            device_category VARCHAR,
            os_browser VARCHAR,
            utm_source VARCHAR,
            cart_total FLOAT,
            event_id VARCHAR,
            event_timestamp BIGINT,
            event_name VARCHAR,
            page_url VARCHAR,
            product_id VARCHAR,
            category VARCHAR,
            price FLOAT,
            quantity INTEGER,
            transaction_id VARCHAR,
            event_date VARCHAR
        ) PARTITIONED BY (event_date) 
        WITH (
            'connector' = 'filesystem',
            'path' = 's3a://{TARGET_BUCKET}/dlq/',
            'format' = 'parquet',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'sink.rolling-policy.file-size' = '128MB'
        )
    """
    t_env.execute_sql(sink_ddl)
    return dlq_table

def create_s3_parquet_sink(t_env):
    sink_table = "s3_clickstream_parquet"
    source_ddl = f"""
        CREATE TABLE {sink_table}(
            session_id VARCHAR,
            client_id VARCHAR,
            user_id VARCHAR,
            ip_address VARCHAR,
            device_category VARCHAR,
            os_browser VARCHAR,
            utm_source VARCHAR,
            cart_total FLOAT,
            event_id VARCHAR,
            event_timestamp BIGINT,
            event_name VARCHAR,
            page_url VARCHAR,
            product_id VARCHAR,
            category VARCHAR,
            price FLOAT,
            quantity INTEGER,
            transaction_id VARCHAR,
            ts TIMESTAMP_LTZ(3),
            event_date VARCHAR
        ) PARTITIONED BY (event_date)
            WITH(
            'connector' = 'filesystem',
            'path' = 's3a://{TARGET_BUCKET}/raw/',
            'format' = 'parquet',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'sink.rolling-policy.file-size' = '128MB'
            )
        """
    t_env.execute_sql(source_ddl)

    return sink_table

def create_s3_revenue_sink(t_env):
    sink_revenue = "s3_revenue"
    source_ddl = f"""
        CREATE TABLE {sink_revenue}(
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            user_id VARCHAR,
            total_revenue FLOAT,
            event_date VARCHAR
        ) PARTITIONED BY (event_date)
        WITH(
            'connector' = 'filesystem',
            'path' = 's3a://{TARGET_BUCKET}/raw/total_revenue',
            'format' = 'parquet',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'sink.rolling-policy.file-size' = '128MB'
        )

    """
    t_env.execute_sql(source_ddl)

    return sink_revenue

def log_upload_s3():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    flink_parallesim = int(os.environ.get("FLINK_PARALLELISM", 1))
    env.set_parallelism(flink_parallesim)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    sink_table = create_s3_parquet_sink(t_env)
    sink_revenue = create_s3_revenue_sink(t_env)
    sink_dlq = create_kafka_dlq_sink(t_env)

    t_env.execute_sql(f"""
            EXECUTE STATEMENT SET
            BEGIN
                INSERT INTO {sink_table}
                SELECT
                    session_id, client_id, user_id, ip_address, device_category, 
                    os_browser, utm_source, cart_total, event_id, event_timestamp, 
                    event_name, page_url, product_id, category, price, quantity, 
                    transaction_id, 
                    ts,
                    DATE_FORMAT(ts, 'yyyy-MM-dd') AS event_date
                FROM {source_table}
                WHERE user_id IS NOT NULL AND cart_total IS NOT NULL;
                INSERT INTO {sink_revenue}
                SELECT
                    window_start, window_end, user_id, SUM(cart_total) AS total_revenue, DATE_FORMAT(window_start, 'yyyy-MM-dd') AS event_date
                FROM TABLE (
                    TUMBLE(TABLE {source_table}, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
                )
                WHERE event_name = 'purchase' AND user_id IS NOT NULL AND cart_total IS NOT NULL
                GROUP BY window_start, window_end, user_id;
                INSERT INTO {sink_dlq}
                SELECT
                    session_id, client_id, user_id, ip_address, device_category, 
                    os_browser, utm_source, cart_total, event_id, event_timestamp, 
                    event_name, page_url, product_id, category, price, quantity, 
                    transaction_id,
                    DATE_FORMAT(ts, 'yyyy-MM-dd') AS event_date
                FROM {source_table}
                WHERE user_id IS NULL OR cart_total IS NULL;
            END;
    """).wait()

if __name__ == '__main__':
    log_upload_s3()
