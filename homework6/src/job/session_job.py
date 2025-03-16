from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_session_results_sink(t_env):
    table_name = 'taxi_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            session_duration BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "taxi_events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_top_sessions_sink(t_env):
    table_name = 'longest_taxi_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            session_duration BIGINT,
            rank_num BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def session_job():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    try:
        # Create tables
        source_table = create_events_source_kafka(t_env)
        sessions_table = create_session_results_sink(t_env)
        top_sessions_table = create_top_sessions_sink(t_env)
        
        # Find all sessions with 5-minute gap
        sessions_sql = f"""
        INSERT INTO {sessions_table}
        SELECT 
            PULocationID,
            DOLocationID,
            window_start AS session_start,
            window_end AS session_end,
            COUNT(*) AS trip_count,
            TIMESTAMPDIFF(SECOND, window_start, window_end) AS session_duration
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(lpep_dropoff_datetime), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID, DOLocationID
        """
        
        print("Executing session window query...")
        t_env.execute_sql(sessions_sql).wait()
        print("Session aggregation completed")
        
        # Find top sessions by duration for each PU/DO pair
        top_sql = f"""
        INSERT INTO {top_sessions_table}
        SELECT 
            PULocationID,
            DOLocationID,
            session_start,
            session_end,
            trip_count,
            session_duration,
            rank_num
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY PULocationID, DOLocationID 
                    ORDER BY session_duration DESC
                ) AS rank_num
            FROM {sessions_table}
        )
        WHERE rank_num <= 3
        """
        
        print("Finding longest sessions...")
        t_env.execute_sql(top_sql).wait()
        print("Top sessions analysis completed")
        
    except Exception as e:
        import traceback
        print("Session window analysis failed:", str(e))
        print(traceback.format_exc())

if __name__ == '__main__':
    session_job()