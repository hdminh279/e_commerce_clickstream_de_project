from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os

TARGET_BUCKET = os.getenv("TARGET_S3_BUCKET")

def create_events_source_kafka(t_env):
    table_name = "clickstream_raw"
    source_dll = f"""
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
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        )
            WITH(
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'ecommerce_clickstream',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
            )
        """
    
    t_env.execute_sql(source_dll)
    return table_name

def create_s3_parquet_sink(t_env):
    sink_table = "s3_clickstream_parquet"
    source_dll = f"""
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
    t_env.execute_sql(source_dll)

    return sink_table

def log_upload_s3():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    sink_table = create_s3_parquet_sink(t_env)

    t_env.execute_sql(f"""
            INSERT INTO {sink_table}
            SELECT
                session_id, client_id, user_id, ip_address, device_category, 
                os_browser, utm_source, cart_total, event_id, event_timestamp, 
                event_name, page_url, product_id, category, price, quantity, 
                transaction_id, 
                ts,
                DATE_FORMAT(ts, 'yyyy-MM-dd') AS event_date
            FROM {source_table}
    """).wait()


if __name__ == '__main__':
    log_upload_s3()