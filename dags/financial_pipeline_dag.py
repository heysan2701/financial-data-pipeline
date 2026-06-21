from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SPARK_SUBMIT   = "/opt/spark/bin/spark-submit"
SPARK_MASTER   = "local[*]"

BASE_JARS = (
    "/root/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/root/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
    "/root/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.4.jar,"
    "/root/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.4.jar,"
    "/root/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar,"
    "/root/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
)

GOLD_JARS = (
    BASE_JARS + ","
    "/root/.ivy2/jars/org.postgresql_postgresql-42.6.0.jar,"
    "/root/.ivy2/jars/org.checkerframework_checker-qual-3.31.0.jar"
)

KAFKA_JARS = (
    BASE_JARS + ","
    "/root/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.4.1.jar,"
    "/root/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.4.1.jar,"
    "/root/.ivy2/jars/org.apache.kafka_kafka-clients-3.3.2.jar,"
    "/root/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar,"
    "/root/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar,"
    "/root/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.10.1.jar,"
    "/root/.ivy2/jars/org.slf4j_slf4j-api-2.0.6.jar"
)

SCRIPTS_DIR = "/opt/spark/spark_jobs"
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "financial-pipeline",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 6, 21),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,                          # retry twice on failure
    "retry_delay":      timedelta(minutes=5),       # wait 5 min between retries
}

with DAG(
    dag_id="financial_data_pipeline",
    default_args=default_args,
    description="Kafka → Bronze → Silver → Gold every 30 minutes",
    schedule_interval="*/30 * * * *",              # every 30 minutes
    catchup=False,                                  # don't backfill missed runs
    max_active_runs=1,                              # only one run at a time
    tags=["financial", "lakehouse", "medallion"],
) as dag:

    # ── TASK 1: Health Checks ─────────────────────────────────────────────────
    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command=(
            "docker exec financial-data-pipeline-kafka-1 "
            "kafka-topics --list --bootstrap-server localhost:9092 "
            "| grep -E 'nav_raw|txn_raw' "
            "&& echo '✅ Kafka topics healthy' "
            "|| (echo '❌ Kafka topics missing!' && exit 1)"
        ),
    )

    check_minio = BashOperator(
        task_id="check_minio_health",
        bash_command=(
            "curl -sf http://minio:9000/minio/health/live "
            "&& echo '✅ MinIO healthy' "
            "|| (echo '❌ MinIO not reachable!' && exit 1)"
        ),
    )

    check_postgres = BashOperator(
        task_id="check_postgres_health",
        bash_command=(
            "docker exec financial-data-pipeline-gold-dw-1 "
            "pg_isready -U dw_user -d financial_dw "
            "&& echo '✅ PostgreSQL healthy' "
            "|| (echo '❌ PostgreSQL not reachable!' && exit 1)"
        ),
    )

    # ── TASK 2: Bronze Layer ──────────────────────────────────────────────────
    bronze_ingestion = BashOperator(
        task_id="bronze_kafka_to_minio",
        bash_command=(
            f"docker exec financial-data-pipeline-spark-worker-1 "
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} "
            f"--jars {KAFKA_JARS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=minio:9000 "
            f"--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            f"{SCRIPTS_DIR}/kafka_to_bronze.py "
            f"&& echo '✅ Bronze layer complete'"
        ),
        execution_timeout=timedelta(minutes=10),    # kill if takes >10 mins
    )

    # ── TASK 3: Silver Layer ──────────────────────────────────────────────────
    silver_transform = BashOperator(
        task_id="silver_bronze_to_silver",
        bash_command=(
            f"docker exec financial-data-pipeline-spark-worker-1 "
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} "
            f"--jars {BASE_JARS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=minio:9000 "
            f"--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            f"{SCRIPTS_DIR}/bronze_to_silver.py "
            f"&& echo '✅ Silver layer complete'"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── TASK 4: Gold Layer ────────────────────────────────────────────────────
    gold_aggregate = BashOperator(
        task_id="gold_silver_to_postgres",
        bash_command=(
            f"docker exec financial-data-pipeline-spark-worker-1 "
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} "
            f"--jars {GOLD_JARS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=minio:9000 "
            f"--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            f"--conf spark.hadoop.fs.s3a.path.style.access=true "
            f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            f"{SCRIPTS_DIR}/silver_to_gold.py "
            f"&& echo '✅ Gold layer complete'"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── TASK 5: Verify Pipeline Success ──────────────────────────────────────
    verify_pipeline = BashOperator(
        task_id="verify_pipeline_success",
        bash_command=(
            "docker exec financial-data-pipeline-gold-dw-1 "
            "psql -U dw_user -d financial_dw -c "
            "'SELECT COUNT(*) as total_funds FROM gold_top_funds;' "
            "&& echo '✅ Pipeline verified successfully!'"
        ),
    )

    # ── TASK 6: Alert on Failure ──────────────────────────────────────────────
    alert_on_failure = BashOperator(
        task_id="alert_on_failure",
        bash_command=(
            "echo '❌ Pipeline FAILED at $(date)! "
            "Check Airflow logs for details.' "
            ">> /opt/airflow/logs/pipeline_failures.log"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,        # only runs if something failed
    )

    # ─── DAG DEPENDENCIES (order of execution) ────────────────────────────────
    #
    #  check_kafka  ──┐
    #  check_minio  ──┼──▶ bronze ──▶ silver ──▶ gold ──▶ verify
    #  check_postgres─┘                                      │
    #                                              alert (if failed)
    #
    [check_kafka, check_minio, check_postgres] >> bronze_ingestion
    bronze_ingestion >> silver_transform
    silver_transform >> gold_aggregate
    gold_aggregate >> verify_pipeline
    [bronze_ingestion, silver_transform, gold_aggregate] >> alert_on_failure

