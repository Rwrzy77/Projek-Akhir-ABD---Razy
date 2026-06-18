"""
Spark Structured Streaming — Kafka Consumer
============================================
Membaca event lowongan kerja dari Kafka, melakukan agregasi real-time,
dan menulis hasil ke stream_outputs/ untuk dashboard.

Contoh:
  python spark_streaming.py

Prasyarat:
  1. docker compose up -d
  2. python kafka_producer.py   (terminal terpisah, atau sudah ada data di topic)
"""

import json
import os
import sys
from datetime import datetime, timezone

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    lit,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_JOBS,
    SPARK_CHECKPOINT_DIR,
    STREAM_KEYWORD_COUNTS,
    STREAM_OUTPUT_DIR,
    STREAM_PLATFORM_COUNTS,
    STREAM_RECENT_JOBS,
    STREAM_SUMMARY,
)

JOB_SCHEMA = StructType([
    StructField("job_title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("job_type", StringType(), True),
    StructField("experience", StringType(), True),
    StructField("education", StringType(), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("keyword", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("post_date", StringType(), True),
    StructField("job_url", StringType(), True),
    StructField("event_time", StringType(), True),
])

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"


def create_spark():
    return (
        SparkSession.builder
        .appName("ITJobKafkaStreaming")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )


def parse_kafka_stream(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_JOBS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), JOB_SCHEMA).alias("job"))
        .select("job.*")
        .withColumn("event_ts", to_timestamp(col("event_time")).cast(TimestampType()))
        .fillna({"platform": "Unknown", "keyword": "Unknown", "country": "Unknown"})
    )


def write_platform_counts(batch_df, _epoch_id):
    if batch_df.rdd.isEmpty():
        return

    agg = (
        batch_df.groupBy("platform")
        .agg(count(lit(1)).alias("stream_count"))
        .orderBy(col("stream_count").desc())
        .toPandas()
    )
    agg.to_csv(STREAM_PLATFORM_COUNTS, sep=";", index=False, encoding="utf-8-sig")
    _update_summary("platform_counts", len(agg))


def write_keyword_counts(batch_df, _epoch_id):
    if batch_df.rdd.isEmpty():
        return

    agg = (
        batch_df.groupBy("keyword")
        .agg(count(lit(1)).alias("stream_count"))
        .orderBy(col("stream_count").desc())
        .limit(20)
        .toPandas()
    )
    agg.to_csv(STREAM_KEYWORD_COUNTS, sep=";", index=False, encoding="utf-8-sig")
    _update_summary("keyword_counts", len(agg))


def write_recent_jobs(batch_df, _epoch_id):
    if batch_df.rdd.isEmpty():
        return

    cols = [
        "event_time", "job_title", "company_name", "platform",
        "keyword", "country", "salary_min", "salary_max",
    ]
    recent = (
        batch_df.select(*cols)
        .orderBy(col("event_time").desc())
        .limit(50)
        .toPandas()
    )
    recent.to_csv(STREAM_RECENT_JOBS, sep=";", index=False, encoding="utf-8-sig")
    _update_summary("recent_jobs", len(recent))


def _update_summary(metric: str, value: int):
    summary = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "kafka_topic": KAFKA_TOPIC_JOBS,
        "kafka_broker": KAFKA_BOOTSTRAP_SERVERS,
        "metrics": {},
    }

    if os.path.exists(STREAM_SUMMARY):
        try:
            with open(STREAM_SUMMARY, encoding="utf-8") as f:
                summary = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    summary["last_updated"] = datetime.now(timezone.utc).isoformat()
    summary.setdefault("metrics", {})[metric] = value

    os.makedirs(STREAM_OUTPUT_DIR, exist_ok=True)
    with open(STREAM_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main():
    os.makedirs(STREAM_OUTPUT_DIR, exist_ok=True)
    os.makedirs(SPARK_CHECKPOINT_DIR, exist_ok=True)

    print("=" * 60)
    print("  ⚡ SPARK STRUCTURED STREAMING — KAFKA CONSUMER")
    print("=" * 60)
    print(f"  Broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic   : {KAFKA_TOPIC_JOBS}")
    print(f"  Output  : {STREAM_OUTPUT_DIR}/")
    print("=" * 60)
    print("  Menunggu event dari Kafka... (Ctrl+C untuk stop)")
    print()

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    jobs_df = parse_kafka_stream(spark)

    checkpoint_base = os.path.join(SPARK_CHECKPOINT_DIR)

    query_platform = (
        jobs_df.writeStream
        .foreachBatch(write_platform_counts)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_base, "platform"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    query_keyword = (
        jobs_df.writeStream
        .foreachBatch(write_keyword_counts)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_base, "keyword"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    query_recent = (
        jobs_df.writeStream
        .foreachBatch(write_recent_jobs)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_base, "recent"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Windowed aggregation: jobs per platform per 1-minute tumbling window
    windowed = (
        jobs_df.withWatermark("event_ts", "2 minutes")
        .groupBy(window(col("event_ts"), "1 minute"), col("platform"))
        .agg(count(lit(1)).alias("jobs_per_minute"))
    )

    def write_windowed(batch_df, _epoch_id):
        if batch_df.rdd.isEmpty():
            return
        pdf = batch_df.orderBy(col("window.start").desc()).limit(20).toPandas()
        path = os.path.join(STREAM_OUTPUT_DIR, "live_windowed_platform.csv")
        pdf.to_csv(path, sep=";", index=False, encoding="utf-8-sig")

    query_window = (
        windowed.writeStream
        .foreachBatch(write_windowed)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(checkpoint_base, "windowed"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Menghentikan streaming queries...")
    finally:
        for q in [query_platform, query_keyword, query_recent, query_window]:
            if q.isActive:
                q.stop()
        spark.stop()
        print("✅ Spark session dihentikan.")


if __name__ == "__main__":
    main()
