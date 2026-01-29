FROM apache/spark:3.4.1

USER root

# Install Python dependencies and build tools
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir pandas pyarrow psycopg2-binary

# Create directories for data lake and checkpoints
RUN mkdir -p /opt/spark/data/lake /opt/spark/checkpoint && \
    chmod -R 777 /opt/spark/data/lake /opt/spark/checkpoint

# Set working directory
WORKDIR /app

# The application code will be mounted as a volume
CMD ["/opt/spark/bin/spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.5.0", \
    "main.py"]
