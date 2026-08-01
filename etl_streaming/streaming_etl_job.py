"""
Streaming ETL job: consumes real estate transaction events from Kafka in
near real time, and continuously writes them into the Data Lake.

HOW THIS DIFFERS FROM THE BATCH JOB (etl_batch/batch_etl_job.py):
The batch job reads a bounded file once, computes a result, and stops.
This job defines a QUERY that keeps running indefinitely: Spark
repeatedly checks Kafka for new messages, processes them in small
"micro-batches", and writes the result -- all while the query is active.
This is Spark Structured Streaming's core idea: streaming is just batch
processing run repeatedly and incrementally, using the exact same
DataFrame API you already know from the batch job.

WHY WE LAND THIS IN THE LAKE ONLY (not the Warehouse) FOR NOW:
Loading streaming events into the star schema fact table would require
looking up (or creating on the fly) dimension keys for values that might
not exist yet -- a harder problem than the batch case, where all
dimension values were known upfront from the full historical file. We
keep this stage focused on proving the streaming MECHANICS work end to
end (Kafka -> Spark Streaming -> Lake); merging streaming data into the
Warehouse is a natural next increment, not a blocker for this Palier.

CHECKPOINTING: Structured Streaming tracks its progress (which Kafka
offsets have already been processed) in a checkpoint location. This is
what allows the job to be stopped and restarted without either losing
events or reprocessing the same ones twice.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "real-estate-events"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
STREAMING_ZONE_PATH = "s3a://raw-zone/streaming_transactions/"
CHECKPOINT_PATH = "s3a://raw-zone/_checkpoints/streaming_transactions/"

EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("listing_id", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("surface_m2", DoubleType(), True),
    StructField("price_mad", DoubleType(), True),
    StructField("registration_status", StringType(), True),
    StructField("source_office", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("RealEstateStreamingETL")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Kafka connector is very verbose at INFO level

    # Read: connect to the Kafka topic as a streaming source. This does
    # NOT read all messages immediately -- it defines where to read FROM.
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")  # process events already sitting in the topic, not just new ones
        .load()
    )

    # Kafka messages arrive as raw bytes in a "value" column. We parse that
    # JSON payload into structured columns using the schema above -- the
    # same idea as defining a schema for the batch CSV read.
    parsed_stream = (
        raw_stream.select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json(F.col("json_value"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Write: append each micro-batch to the Data Lake as Parquet.
    # trigger(processingTime=...) controls how often Spark checks Kafka for
    # new messages -- every 10 seconds here, a reasonable rate for a demo.
    query = (
        parsed_stream.writeStream.format("parquet")
        .option("path", STREAMING_ZONE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming query started. Writing to {STREAMING_ZONE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()