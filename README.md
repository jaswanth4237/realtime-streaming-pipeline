# Real-Time Data Pipeline with Kafka and Spark Streaming

This project implements a complete real-time data pipeline that ingests user activity events, processes them using Spark Structured Streaming, and distributes the results to multiple sinks (PostgreSQL, Data Lake, and Kafka).

## Architecture

1.  **Data Producer**: A Python script (`producer.py`) simulates user activity events and publishes them to the `user_activity` Kafka topic.
2.  **Kafka & Zookeeper**: Serve as the message broker for ingesting and storing event streams.
3.  **Spark Streaming**: Consumes events, performs windowed aggregations and stateful transformations (sessions), and handles late data using watermarks.
4.  **Sinks**:
    *   **PostgreSQL**: Stores page view counts, active user counts, and session details.
    *   **Data Lake**: All raw events are stored in Parquet format, partitioned by date.
    *   **Kafka**: Enriched events (with processing time) are published to the `enriched_activity` topic.

## Prerequisites

-   Docker and Docker Compose
-   Python 3.x (for running the producer script)
-   `kafka-python` library: `pip install kafka-python`

## Getting Started

1.  **Clone the repository** (or navigate to the project directory).
2.  **Set up environment variables**:
    Create a `.env` file based on `.env.example`:
    ```bash
    cp .env.example .env
    ```
3.  **Install producer dependencies**:
    ```bash
    pip install kafka-python
    ```
4.  **Start the infrastructure**:
    ```bash
    docker-compose up -d
    ```
    Wait for all services to become healthy. You can check the status with `docker-compose ps`.

5.  **Start the Data Producer** (in a new terminal):
    ```bash
    python producer.py
    ```

## Verification

### 1. Check PostgreSQL Tables
Connect to the database and query the analytics tables to verify windowed aggregations and stateful sessions:
```bash
docker exec -it db psql -U user -d stream_data
```
```sql
-- Requirement 5: Check 1-minute tumbling window page views
SELECT * FROM page_view_counts LIMIT 10;

-- Requirement 6: Check 5-minute sliding window active users
SELECT * FROM active_users LIMIT 5;

-- Requirement 7 & 9: Check stateful user sessions (session_start to session_end)
SELECT * FROM user_sessions LIMIT 10;
```

### 2. Inspect Data Lake (Requirement 10)
Check the local `data/lake` directory for Parquet files partitioned by date:
```bash
# On Linux/macOS
ls -R data/lake/event_date=*

# On Windows (PowerShell)
Get-ChildItem -Path data/lake -Recurse
```

### 3. Consume Enriched Events (Requirement 11)
Use a Kafka consumer to see the events enriched with `processing_time`:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic enriched_activity --from-beginning --max-messages 10
```

### 4. Test Late Data (Requirement 8)
The `producer.py` script automatically generates "LATE" events (3 minutes in the past) with a 5% probability. Because the watermark is set to 2 minutes, these events should be dropped and **not** show up in the windowed counts.
```bash
# Monitor producer output for LATE events
python producer.py | grep "LATE"
```

## Key Features

-   **Windowing**: 1-minute tumbling windows for page views and 5-minute sliding windows (1-minute slide) for active users.
-   **Stateful Transformations**: Tracks session duration from `session_start` to `session_end` with a 15-minute timeout using `applyInPandasWithState`.
-   **Watermarking**: Handles data arriving up to 2 minutes late, dropping events older than the watermark threshold.
-   **Idempotency**: Implements UPSERT logic (INSERT...ON CONFLICT) for PostgreSQL sinks to ensure exactly-once semantics during retries.
-   **Multiple Sinks**: 
    - PostgreSQL for real-time dashboards and analytics
    - Data Lake (Parquet) partitioned by event date for historical analysis
    - Kafka topic for downstream streaming consumers
-   **Late Data Handling**: Events with timestamps older than 2 minutes are automatically dropped by the watermark.

## Implementation Notes

### Schema Management
The application defines the event schema directly within the Spark job. For production systems, consider integrating Confluent Schema Registry for schema evolution and validation.

### Exactly-Once Semantics
The implementation uses idempotent writes to PostgreSQL via UPSERT operations (`INSERT...ON CONFLICT DO UPDATE`). This ensures that if Spark retries a batch, duplicate rows are not created—instead, existing rows are updated.

### State Management for Sessions
Session tracking uses Spark's `applyInPandasWithState` API, which maintains state per user. The state includes session start time, end time, and duration. Sessions that don't receive a `session_end` event within 15 minutes are automatically timed out.

### Kafka Integration
The application connects to Kafka using the `spark-sql-kafka-0-10` package. For Docker environments, the Kafka hostname is `kafka:9092` (internal) and `localhost:29092` (external for producers).

## Troubleshooting

-   If Spark fails to connect to Kafka or DB, check the logs: `docker-compose logs spark-app`.
-   Ensure the `data/lake` and `spark/checkpoint` directories have write permissions.
