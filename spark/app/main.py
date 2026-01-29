import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, to_timestamp, current_timestamp, 
    struct, to_json, approx_count_distinct, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DB_URL = os.environ.get('DB_URL')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

def main():
    spark = SparkSession.builder \
        .appName("RealTimeStreamingPipeline") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define schema for incoming Kafka messages
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_type", StringType(), True)
    ])

    # 1. Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "user_activity") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and apply schema, convert event_time to Timestamp
    events_df = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd")) \
        .withWatermark("event_time", "2 minutes")

    # 2. Page View Counts (1-minute tumbling window)
    page_view_counts = events_df.filter(col("event_type") == "page_view") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("page_url")
        ).count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "page_url",
            col("count").alias("view_count")
        )

    # 3. Active Users (5-minute sliding window, 1-minute slide)
    active_users = events_df \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute")
        ).agg(approx_count_distinct("user_id").alias("active_user_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_user_count"
        )

    # 4. Enriched events for Kafka Sink
    enriched_events = events_df \
        .withColumn("processing_time", current_timestamp().cast("string")) \
        .withColumn("event_time", col("event_time").cast("string")) \
        .select(to_json(struct(
            "event_time", "user_id", "page_url", "event_type", "processing_time"
        )).alias("value"))

    # Helper function for UPSERT to PostgreSQL
    def write_to_postgres(df, batch_id, table_name, conflict_cols, update_cols):
        try:
            row_count = df.count()
            if row_count == 0:
                print(f"Batch {batch_id} for {table_name}: No rows to process")
                return
            
            print(f"Batch {batch_id} for {table_name}: processing {row_count} rows")
            
            # Convert Spark DataFrame to Pandas
            pandas_df = df.toPandas()
            
            # Convert timestamp columns to string to avoid serialization issues
            for col_name in pandas_df.columns:
                if 'time' in col_name or 'window' in col_name:
                    pandas_df[col_name] = pandas_df[col_name].astype(str)
            
            # Connection details
            conn = psycopg2.connect(
                host="db",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            
            # Build UPSERT query
            cols = list(pandas_df.columns)
            cols_str = ", ".join(cols)
            placeholders = ", ".join(["%s"] * len(cols))
            conflict_str = ", ".join(conflict_cols)
            update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            
            upsert_sql = f"""
                INSERT INTO {table_name} ({cols_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str})
                DO UPDATE SET {update_str}
            """
            
            # Convert DataFrame rows to tuples
            records = [tuple(row) for row in pandas_df.values]
            
            # Execute batch insert
            for record in records:
                cursor.execute(upsert_sql, record)
            
            conn.commit()
            print(f"Batch {batch_id} for {table_name}: Successfully UPSERTed {len(records)} records")
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Error in UPSERT for {table_name}: {e}")
            import traceback
            traceback.print_exc()

    # 5. Sinks: PostgreSQL
    page_views_query = page_view_counts.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, batch_id: write_to_postgres(
            df, batch_id, "page_view_counts", ["window_start", "page_url"], ["view_count", "window_end"]
        )) \
        .option("checkpointLocation", "/opt/spark/checkpoint/page_views") \
        .start()

    active_users_query = active_users.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, batch_id: write_to_postgres(
            df, batch_id, "active_users", ["window_start"], ["active_user_count", "window_end"]
        )) \
        .option("checkpointLocation", "/opt/spark/checkpoint/active_users") \
        .start()

    # 6. Sink: Data Lake (Parquet)
    data_lake_query = events_df.writeStream \
        .format("parquet") \
        .option("path", "/opt/spark/data/lake") \
        .option("checkpointLocation", "/opt/spark/checkpoint/data_lake") \
        .partitionBy("event_date") \
        .start()

    # 7. Sink: Kafka (enriched_activity)
    kafka_sink_query = enriched_events.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "enriched_activity") \
        .option("checkpointLocation", "/opt/spark/checkpoint/kafka_enriched") \
        .start()

    # 8. Stateful session transformation
    session_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_start_time", TimestampType(), True),
        StructField("session_end_time", TimestampType(), True),
        StructField("session_duration_seconds", LongType(), True)
    ])

    state_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_start_time", TimestampType(), True),
        StructField("session_end_time", TimestampType(), True),
        StructField("session_duration_seconds", LongType(), True)
    ])

    def update_session_state(key, pdf_iterator, state):
        """
        Update session state based on incoming events.
        Sessions track from session_start to session_end event.
        pdf_iterator is an Iterator[pd.DataFrame] in Spark 3.4+
        """
        user_id = key[0] if isinstance(key, tuple) else key
        
        # Handle timeout
        if state.hasTimedOut:
            if state.exists:
                existing_state = state.get
                session_start = existing_state[1]
                if session_start is not None:
                    # Emitting the session as "timed out"
                    # Duration is calculated up to the last event or current time
                    timeout_duration = 900 # 15 mins
                    yield pd.DataFrame([{
                        'user_id': user_id,
                        'session_start_time': session_start,
                        'session_end_time': None, # Session timed out
                        'session_duration_seconds': timeout_duration
                    }])
                state.remove()
            return

        # Consolidate iterator into a single DataFrame
        pdf_list = list(pdf_iterator)
        if not pdf_list:
            return
            
        pdf = pd.concat(pdf_list)
        
        # Get existing state if it exists
        existing_state = None
        if state.exists:
            try:
                existing_state = state.get
            except:
                existing_state = None

        # Initialize with existing state or None
        session_start = None
        session_end = None
        duration = None
        
        if existing_state:
            session_start = existing_state[1]
            session_end = existing_state[2]
            duration = existing_state[3]

        # Process incoming events
        pdf = pdf.sort_values("event_time")
        output_rows = []
        
        for _, row in pdf.iterrows():
            event_type = row.get('event_type')
            event_time = row.get('event_time')
            
            if event_type == 'session_start':
                session_start = event_time
                session_end = None
                duration = None
            elif event_type == 'session_end' and session_start is not None:
                session_end = event_time
                # Calculate duration in seconds
                duration = int((session_end - session_start).total_seconds())
                # Emit the completed session
                output_rows.append({
                    'user_id': user_id,
                    'session_start_time': session_start,
                    'session_end_time': session_end,
                    'session_duration_seconds': duration
                })
                # Reset for next session
                session_start = None
                session_end = None
                duration = None

        # Update state for ongoing session
        new_state = (user_id, session_start, session_end, duration)
        state.update(new_state)
        
        # Set timeout to 15 minutes
        if not pdf.empty:
            max_time = pdf['event_time'].max()
            timeout_ms = int(max_time.timestamp() * 1000) + (15 * 60 * 1000)
            state.setTimeoutTimestamp(timeout_ms)

        # Return completed sessions as iterator
        if output_rows:
            yield pd.DataFrame(output_rows)
        else:
            yield pd.DataFrame(columns=['user_id', 'session_start_time', 'session_end_time', 'session_duration_seconds'])

    session_stream = events_df \
        .filter(col("event_type").isin("session_start", "session_end")) \
        .groupBy("user_id") \
        .applyInPandasWithState(
            update_session_state,
            outputStructType=session_schema,
            stateStructType=state_schema,
            outputMode="update",
            timeoutConf="EventTimeTimeout"
        )

    sessions_query = session_stream.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, batch_id: write_to_postgres(
            df, batch_id, "user_sessions", ["user_id"], ["session_start_time", "session_end_time", "session_duration_seconds"]
        )) \
        .option("checkpointLocation", "/opt/spark/checkpoint/user_sessions") \
        .start()

    # Wait for completion
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
