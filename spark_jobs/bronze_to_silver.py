import os
import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET         = "financial-lakehouse"
# ──────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .config(
        "spark.jars.packages",
        (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    )
    .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access",  "true")
    .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

# Schema for NAV json_payload
nav_schema = StructType([
    StructField("timestamp",   StringType(),  True),
    StructField("scheme_code", IntegerType(), True),
    StructField("scheme_name", StringType(),  True),
    StructField("nav",         DoubleType(),  True),
])

# Schema for Transaction json_payload
txn_schema = StructType([
    StructField("transaction_id",   StringType(),  True),
    StructField("timestamp",        StringType(),  True),
    StructField("scheme_code",      IntegerType(), True),
    StructField("transaction_type", StringType(),  True),
    StructField("amount",           DoubleType(),  True),
    StructField("status",           StringType(),  True),
])

# ─── NAV: BRONZE → SILVER ─────────────────────────────────────────────────────

def process_nav():
    print("🔄  Processing NAV data: Bronze → Silver ...")

    bronze_path = f"s3a://{BUCKET}/bronze/nav_data/"
    silver_path = f"s3a://{BUCKET}/silver/nav_data/"

    # 1. Read raw Bronze Parquet
    raw_df = spark.read.parquet(bronze_path)
    print(f"   📥 Raw NAV records read: {raw_df.count()}")

    # 2. Parse json_payload string into proper columns
    parsed_df = raw_df.withColumn(
        "parsed", F.from_json(F.col("json_payload"), nav_schema)
    )

    # 3. Extract fields, cast types, rename clearly
    clean_df = parsed_df.select(
        F.col("parsed.scheme_code").cast(IntegerType()).alias("scheme_code"),
        F.col("parsed.scheme_name").alias("scheme_name"),
        F.col("parsed.nav").cast(DoubleType()).alias("nav"),
        F.to_timestamp(F.col("parsed.timestamp")).alias("event_timestamp"),
        F.col("ingestion_date"),
        # Audit columns
        F.col("topic").alias("kafka_topic"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("processed_at"),
    )

    # 4. Data quality — drop rows where critical fields are null
    before = clean_df.count()
    clean_df = clean_df.filter(
        F.col("scheme_code").isNotNull() &
        F.col("nav").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )
    after = clean_df.count()
    print(f"   🧹 Dropped {before - after} null/invalid NAV records")

    # 5. Remove duplicates based on scheme_code + event_timestamp
    clean_df = clean_df.dropDuplicates(["scheme_code", "event_timestamp"])
    print(f"   ✅ Clean NAV records after dedup: {clean_df.count()}")

    # 6. Validate NAV values — must be positive
    invalid_nav = clean_df.filter(F.col("nav") <= 0).count()
    if invalid_nav > 0:
        print(f"   ⚠️  Found {invalid_nav} records with NAV <= 0, dropping them")
        clean_df = clean_df.filter(F.col("nav") > 0)

    # 7. Write to Silver layer partitioned by ingestion_date
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(silver_path)
    )
    print(f"   💾 NAV Silver data written to: {silver_path}")
    print()

    return clean_df


# ─── TRANSACTIONS: BRONZE → SILVER ────────────────────────────────────────────

def process_transactions():
    print("🔄  Processing Transaction data: Bronze → Silver ...")

    bronze_path = f"s3a://{BUCKET}/bronze/transaction_data/"
    silver_path = f"s3a://{BUCKET}/silver/transaction_data/"

    # 1. Read raw Bronze Parquet
    raw_df = spark.read.parquet(bronze_path)
    print(f"   📥 Raw Transaction records read: {raw_df.count()}")

    # 2. Parse json_payload string into proper columns
    parsed_df = raw_df.withColumn(
        "parsed", F.from_json(F.col("json_payload"), txn_schema)
    )

    # 3. Extract fields, cast types, rename clearly
    clean_df = parsed_df.select(
        F.col("parsed.transaction_id").alias("transaction_id"),
        F.col("parsed.scheme_code").cast(IntegerType()).alias("scheme_code"),
        F.col("parsed.transaction_type").alias("transaction_type"),
        F.col("parsed.amount").cast(DoubleType()).alias("amount"),
        F.col("parsed.status").alias("status"),
        F.to_timestamp(F.col("parsed.timestamp")).alias("event_timestamp"),
        F.col("ingestion_date"),
        # Audit columns
        F.col("topic").alias("kafka_topic"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("processed_at"),
    )

    # 4. Data quality — drop rows where critical fields are null
    before = clean_df.count()
    clean_df = clean_df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("scheme_code").isNotNull() &
        F.col("amount").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )
    after = clean_df.count()
    print(f"   🧹 Dropped {before - after} null/invalid Transaction records")

    # 5. Remove duplicates based on transaction_id
    clean_df = clean_df.dropDuplicates(["transaction_id"])
    print(f"   ✅ Clean Transaction records after dedup: {clean_df.count()}")

    # 6. Validate — amount must be positive
    invalid_amt = clean_df.filter(F.col("amount") <= 0).count()
    if invalid_amt > 0:
        print(f"   ⚠️  Found {invalid_amt} records with amount <= 0, dropping them")
        clean_df = clean_df.filter(F.col("amount") > 0)

    # 7. Validate — transaction_type must be BUY, SELL, or REDEEM only
    valid_types = ["BUY", "SELL", "REDEEM"]
    invalid_type = clean_df.filter(~F.col("transaction_type").isin(valid_types)).count()
    if invalid_type > 0:
        print(f"   ⚠️  Found {invalid_type} records with invalid transaction_type, dropping them")
        clean_df = clean_df.filter(F.col("transaction_type").isin(valid_types))

    # 8. Write to Silver layer partitioned by ingestion_date
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(silver_path)
    )
    print(f"   💾 Transaction Silver data written to: {silver_path}")
    print()

    return clean_df


# ─── SUMMARY ──────────────────────────────────────────────────────────────────

def print_summary(nav_df, txn_df):
    print("=" * 60)
    print("📊  SILVER LAYER SUMMARY")
    print("=" * 60)

    print("\n📈 NAV Data Sample:")
    nav_df.show(5, truncate=False)

    print("\n💸 Transaction Data Sample:")
    txn_df.show(5, truncate=False)

    print("\n📅 NAV by Date:")
    nav_df.groupBy("ingestion_date").count().orderBy("ingestion_date").show()

    print("\n📅 Transactions by Date and Type:")
    (
        txn_df.groupBy("ingestion_date", "transaction_type")
        .agg(
            F.count("*").alias("count"),
            F.round(F.sum("amount"), 2).alias("total_amount")
        )
        .orderBy("ingestion_date", "transaction_type")
        .show()
    )

    print("=" * 60)
    print("✅  Silver layer processing complete!")
    print(f"    NAV Silver      → s3a://{BUCKET}/silver/nav_data/")
    print(f"    Transaction Silver → s3a://{BUCKET}/silver/transaction_data/")
    print("=" * 60)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n🚀  Starting Bronze → Silver transformation ...\n")

    nav_df = process_nav()
    txn_df = process_transactions()

    print_summary(nav_df, txn_df)

    spark.stop()
    print("\n✅  Spark stopped cleanly.")
