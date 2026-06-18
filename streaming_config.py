"""Shared configuration for Kafka + Spark Streaming pipeline."""

import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_JOBS = os.getenv("KAFKA_TOPIC_JOBS", "it-jobs-stream")

# Spark Streaming outputs (written by spark_streaming.py, read by app.py)
STREAM_OUTPUT_DIR = os.getenv("STREAM_OUTPUT_DIR", "stream_outputs")
STREAM_PLATFORM_COUNTS = os.path.join(STREAM_OUTPUT_DIR, "live_platform_counts.csv")
STREAM_KEYWORD_COUNTS = os.path.join(STREAM_OUTPUT_DIR, "live_keyword_counts.csv")
STREAM_RECENT_JOBS = os.path.join(STREAM_OUTPUT_DIR, "live_recent_jobs.csv")
STREAM_SUMMARY = os.path.join(STREAM_OUTPUT_DIR, "stream_summary.json")

# Producer defaults
DEFAULT_CSV_SOURCE = "merged_it_jobs.csv"
DEFAULT_REPLAY_DELAY_SEC = 0.05

# Spark checkpoint location
SPARK_CHECKPOINT_DIR = os.path.join(STREAM_OUTPUT_DIR, "checkpoints")
