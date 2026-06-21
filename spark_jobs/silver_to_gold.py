import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET          = "financial-lakehouse"

PG_HOST         = os.getenv("PG_HOST",     "gold-dw")
PG_PORT         = os.getenv("PG_PORT",     "5432")
PG_DB           = os.getenv("PG_DB",       "financial_dw")
PG_USER         = os.getenv("PG_USER",     "dw_user")
PG_PASSWORD     = os.getenv("PG_PASSWORD", "dw_password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}
# ──────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .config(
        "spark.jars.packages",
        (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0"
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


# ─── READ SILVER DATA ─────────────────────────────────────────────────────────

def read_silver():
    print("📖  Reading Silver layer data ...")

    nav_df = spark.read.parquet(f"s3a://{BUCKET}/silver/nav_data/")
    txn_df = spark.read.parquet(f"s3a://{BUCKET}/silver/transaction_data/")

    print(f"   NAV records:         {nav_df.count()}")
    print(f"   Transaction records: {txn_df.count()}")
    print()

    return nav_df, txn_df


# ─── GOLD TABLE 1: AUM BY FUND PER DAY ───────────────────────────────────────

def compute_aum(nav_df, txn_df):
    """
    AUM (Assets Under Management) = Total units held × Latest NAV
    We approximate: AUM per day = net transaction amount per fund per day
    (BUY adds to AUM, SELL subtracts from AUM)
    """
    print("🥇  Computing Gold Table 1: AUM by Fund per Day ...")

    # Net flow per fund per day
    aum_df = (
        txn_df
        .withColumn(
            "signed_amount",
            F.when(F.col("transaction_type") == "BUY",  F.col("amount"))
             .when(F.col("transaction_type") == "SELL", -F.col("amount"))
             .otherwise(0)
        )
        .groupBy("scheme_code", "ingestion_date")
        .agg(
            F.round(F.sum("signed_amount"), 2).alias("net_aum"),
            F.round(F.sum(
                F.when(F.col("transaction_type") == "BUY", F.col("amount")).otherwise(0)
            ), 2).alias("total_buy_amount"),
            F.round(F.sum(
                F.when(F.col("transaction_type") == "SELL", F.col("amount")).otherwise(0)
            ), 2).alias("total_sell_amount"),
            F.count("*").alias("total_transactions"),
        )
        .orderBy("ingestion_date", "scheme_code")
        .withColumn("processed_at", F.current_timestamp())
    )

    aum_df.show(10, truncate=False)
    return aum_df


# ─── GOLD TABLE 2: NAV GROWTH / FUND PERFORMANCE ─────────────────────────────

def compute_nav_performance(nav_df):
    """
    For each fund per day:
    - Opening NAV (first NAV of the day)
    - Closing NAV (last NAV of the day)
    - Daily return % = (closing - opening) / opening * 100
    - Min/Max NAV of the day
    """
    print("🥇  Computing Gold Table 2: NAV Growth / Fund Performance ...")

    # Window to get first and last NAV per fund per day
    window_first = Window.partitionBy("scheme_code", "ingestion_date").orderBy("event_timestamp")
    window_last  = Window.partitionBy("scheme_code", "ingestion_date").orderBy(F.desc("event_timestamp"))

    nav_perf_df = (
        nav_df
        .withColumn("opening_nav", F.first("nav").over(window_first))
        .withColumn("closing_nav", F.first("nav").over(window_last))
        .groupBy("scheme_code", "scheme_name", "ingestion_date")
        .agg(
            F.round(F.first("opening_nav"), 4).alias("opening_nav"),
            F.round(F.first("closing_nav"), 4).alias("closing_nav"),
            F.round(F.min("nav"), 4).alias("min_nav"),
            F.round(F.max("nav"), 4).alias("max_nav"),
            F.round(F.avg("nav"), 4).alias("avg_nav"),
            F.count("*").alias("nav_updates"),
        )
        .withColumn(
            "daily_return_pct",
            F.round(
                (F.col("closing_nav") - F.col("opening_nav")) / F.col("opening_nav") * 100,
                4
            )
        )
        .orderBy("ingestion_date", F.desc("daily_return_pct"))
        .withColumn("processed_at", F.current_timestamp())
    )

    nav_perf_df.show(10, truncate=False)
    return nav_perf_df


# ─── GOLD TABLE 3: TRANSACTION VOLUME BY TYPE ─────────────────────────────────

def compute_txn_volume(txn_df):
    """
    Per day per transaction type:
    - Count of transactions
    - Total amount
    - Average transaction size
    - Min / Max transaction
    """
    print("🥇  Computing Gold Table 3: Transaction Volume by Type ...")

    txn_vol_df = (
        txn_df
        .groupBy("ingestion_date", "transaction_type", "scheme_code")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"),  2).alias("total_amount"),
            F.round(F.avg("amount"),  2).alias("avg_amount"),
            F.round(F.min("amount"),  2).alias("min_amount"),
            F.round(F.max("amount"),  2).alias("max_amount"),
        )
        .orderBy("ingestion_date", "transaction_type", "scheme_code")
        .withColumn("processed_at", F.current_timestamp())
    )

    txn_vol_df.show(10, truncate=False)
    return txn_vol_df


# ─── GOLD TABLE 4: TOP PERFORMING FUNDS ──────────────────────────────────────

def compute_top_funds(nav_df):
    """
    Rank funds by:
    - Best daily return %
    - Highest average NAV
    - Most consistent (lowest NAV volatility)
    """
    print("🥇  Computing Gold Table 4: Top Performing Funds ...")

    window_first = Window.partitionBy("scheme_code", "ingestion_date").orderBy("event_timestamp")
    window_last  = Window.partitionBy("scheme_code", "ingestion_date").orderBy(F.desc("event_timestamp"))

    # Daily return per fund
    daily_returns = (
        nav_df
        .withColumn("opening_nav", F.first("nav").over(window_first))
        .withColumn("closing_nav", F.first("nav").over(window_last))
        .groupBy("scheme_code", "scheme_name", "ingestion_date")
        .agg(
            F.round(F.first("opening_nav"), 4).alias("opening_nav"),
            F.round(F.first("closing_nav"), 4).alias("closing_nav"),
            F.round(F.avg("nav"),           4).alias("avg_nav"),
            F.round(F.stddev("nav"),        4).alias("nav_volatility"),
        )
        .withColumn(
            "daily_return_pct",
            F.round(
                (F.col("closing_nav") - F.col("opening_nav")) / F.col("opening_nav") * 100,
                4
            )
        )
    )

    # Overall fund ranking across all dates
    top_funds_df = (
        daily_returns
        .groupBy("scheme_code", "scheme_name")
        .agg(
            F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return_pct"),
            F.round(F.max("daily_return_pct"), 4).alias("best_daily_return_pct"),
            F.round(F.avg("avg_nav"),          4).alias("avg_nav"),
            F.round(F.avg("nav_volatility"),   4).alias("avg_volatility"),
            F.count("*").alias("trading_days"),
        )
        .withColumn(
            "rank",
            F.rank().over(
                Window.orderBy(F.desc("avg_daily_return_pct"))
            )
        )
        .orderBy("rank")
        .withColumn("processed_at", F.current_timestamp())
    )

    top_funds_df.show(truncate=False)
    return top_funds_df


# ─── WRITE TO POSTGRESQL ──────────────────────────────────────────────────────

def write_to_postgres(df, table_name):
    print(f"   💾  Writing to PostgreSQL table: {table_name} ...")
    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=table_name,
            mode="overwrite",
            properties=JDBC_PROPS,
        )
    )
    print(f"   ✅  {table_name} written successfully!")
    print()


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n🚀  Starting Silver → Gold transformation ...\n")

    # Read Silver
    nav_df, txn_df = read_silver()

    # Compute all 4 Gold tables
    aum_df      = compute_aum(nav_df, txn_df)
    nav_perf_df = compute_nav_performance(nav_df)
    txn_vol_df  = compute_txn_volume(txn_df)
    top_funds_df = compute_top_funds(nav_df)

    # Write all to PostgreSQL
    print("\n📤  Writing Gold tables to PostgreSQL ...\n")
    write_to_postgres(aum_df,       "gold_aum_by_fund")
    write_to_postgres(nav_perf_df,  "gold_nav_performance")
    write_to_postgres(txn_vol_df,   "gold_txn_volume")
    write_to_postgres(top_funds_df, "gold_top_funds")

    print("=" * 60)
    print("📊  GOLD LAYER COMPLETE!")
    print("=" * 60)
    print("   gold_aum_by_fund      → AUM per fund per day")
    print("   gold_nav_performance  → NAV growth & daily returns")
    print("   gold_txn_volume       → Transaction volume by type")
    print("   gold_top_funds        → Fund rankings & performance")
    print("=" * 60)

    spark.stop()
    print("\n✅  Spark stopped cleanly.")