"""
Kafka Producer — IT Job Market Streaming
==========================================
Mempublish event lowongan kerja ke Kafka topic.

Mode:
  replay  — baca merged_it_jobs.csv dan kirim ke Kafka (simulasi real-time, default)
  single  — kirim satu event JSON dari command line (untuk testing)

Contoh:
  python kafka_producer.py
  python kafka_producer.py --delay 0.1 --limit 500
  python kafka_producer.py --csv merged_it_jobs.csv

Prasyarat:
  docker compose up -d
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from streaming_config import (
    DEFAULT_CSV_SOURCE,
    DEFAULT_REPLAY_DELAY_SEC,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_JOBS,
)


def create_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        print(f"❌ Kafka tidak tersedia di {KAFKA_BOOTSTRAP_SERVERS}")
        print("   Jalankan: docker compose up -d")
        sys.exit(1)


def row_to_event(row) -> dict:
    salary_min = row.get("salary_min")
    salary_max = row.get("salary_max")

    if pd.isna(salary_min):
        salary_min = None
    if pd.isna(salary_max):
        salary_max = None

    return {
        "job_title": row.get("job_title"),
        "company_name": row.get("company_name"),
        "location": row.get("location"),
        "country": row.get("country"),
        "job_type": row.get("job_type"),
        "experience": row.get("experience"),
        "education": row.get("education"),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "keyword": row.get("keyword"),
        "platform": row.get("platform"),
        "post_date": row.get("post_date"),
        "job_url": row.get("job_url"),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def publish_event(producer: KafkaProducer, event: dict):
    future = producer.send(KAFKA_TOPIC_JOBS, value=event)
    future.get(timeout=10)


def replay_csv(csv_path: str, delay: float, limit: int | None):
    print("=" * 60)
    print("  🚀 KAFKA PRODUCER — REPLAY MODE")
    print("=" * 60)
    print(f"  Broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic   : {KAFKA_TOPIC_JOBS}")
    print(f"  Source  : {csv_path}")
    print(f"  Delay   : {delay}s per event")
    print("=" * 60)

    df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig", low_memory=False)
    if limit:
        df = df.head(limit)

    producer = create_producer()
    total = len(df)
    sent = 0

    try:
        for _, row in df.iterrows():
            event = row_to_event(row)
            publish_event(producer, event)
            sent += 1
            if sent % 100 == 0 or sent == total:
                print(f"  📤 Terkirim: {sent:,}/{total:,}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n  ⏹️  Dihentikan oleh user.")
    finally:
        producer.flush()
        producer.close()

    print(f"\n✅ Selesai. {sent:,} event dipublish ke topic '{KAFKA_TOPIC_JOBS}'.")


def main():
    parser = argparse.ArgumentParser(description="Publish IT job events to Kafka")
    parser.add_argument("--csv", default=DEFAULT_CSV_SOURCE, help="CSV source file")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_REPLAY_DELAY_SEC,
        help="Delay antar event (detik)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Batas jumlah event")
    args = parser.parse_args()

    replay_csv(args.csv, args.delay, args.limit)


if __name__ == "__main__":
    main()
