import os
import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── CONFIG ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "minio:9000")  # no http://
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET          = "financial-lakehouse"
# ──────────────────────────────────────────────────────────────────────────────

# Comments cannot appear after a backslash continuation — use parentheses instead
spark = (
    SparkSession.builder
    .appName("KafkaToBronze-Streaming")
    .config(
        "spark.jars.packages",
        (
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    )
    # ── S3A / MinIO ──────────────────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.endpoint",             MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",           MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key",           MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access",    "true")
    .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def consume_topic_to_bronze(topic_name: str, folder_name: str):
    """
    Read a Kafka topic as a structured stream and land raw messages
    into the Bronze layer in MinIO as Parquet, partitioned by event date.
    """
    print(f"🔄  Setting up stream consumer for topic: {topic_name} ...")

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               topic_name)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    processed_stream_df = (
        kafka_stream_df
        .selectExpr(
            "CAST(key   AS STRING) AS key",
            "CAST(value AS STRING) AS json_payload",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
        .withColumn("ingestion_date", F.to_date("timestamp"))  # dynamic — survives midnight
    )

    checkpoint_path = f"s3a://{BUCKET}/checkpoints/{folder_name}"
    output_path     = f"s3a://{BUCKET}/bronze/{folder_name}"

    query = (
        processed_stream_df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .partitionBy("ingestion_date")          # Hive-style partitions written automatically
        .trigger(processingTime="30 seconds")   # flush every 30 s so files appear in MinIO
        .start(output_path)
    )

    return query


def shutdown(sig, frame):
    """Graceful shutdown on SIGINT / SIGTERM."""
    print("\n🛑  Signal received — stopping streams ...")
    for q in active_queries:
        try:
            q.stop()
        except Exception as e:
            print(f"   ⚠️  Error stopping query: {e}")
    spark.stop()
    print("✅  Spark stopped cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    active_queries = [
        consume_topic_to_bronze("nav_raw",  "nav_data"),
        consume_topic_to_bronze("txn_raw",  "transaction_data"),
    ]

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("🚀  Streaming pipelines active. Waiting for data ...")
    print(f"    Bronze layer  → s3a://{BUCKET}/bronze/")
    print(f"    Checkpoints   → s3a://{BUCKET}/checkpoints/")
    print("    Trigger interval: 30 seconds  |  Press Ctrl+C to stop.\n")

    spark.streams.awaitAnyTermination()
