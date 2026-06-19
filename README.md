# Projek Akhir ABD — IT Job Market Big Data Pipeline

Pipeline analisis Big Data lowongan kerja IT dari 5 platform (Glints, LinkedIn,
Indeed, Karir.com, Tech in Asia), mulai dari scraping, cleaning, batch analysis
(Spark), storage (MongoDB), real-time streaming (Kafka + Spark Streaming),
hingga dashboard interaktif (Streamlit).

## Arsitektur

```
Scraping → Cleaning & Merging → Batch Analysis (PySpark) → MongoDB
                                        ↓
                     Kafka Producer → Spark Structured Streaming → Streamlit Dashboard
```

## Struktur File

### Scraping
- `scrap_glints.py` — Selenium, glints.com
- `scrap_indeed.py` — requests/Selenium, id.indeed.com
- `scrap_karir.py` — requests + auth token, karir.com (gateway API)
- `scrap_linkedin.py` — Selenium, linkedin.com/jobs
- `scrap_techinasia.py` — requests, Algolia search index techinasia
- `test_indeed.py`, `test_karir_auth.py`, `test_karir_token.py`, `test_uc.py` — testing koneksi/auth

### Data Mentah
- `data_indeed_it_jobs.csv`, `data_loker_glints.csv`, `karir_dataset_master.csv`,
  `linkedin_jobs_Ribuan.csv`, `techinasia_it_massive.csv`

Skema kolom tiap sumber berbeda-beda (bahasa, nama kolom, format gaji).

### Cleaning & Merging
- `clean_data.py` — normalisasi skema antar platform & parsing gaji multi-format/currency
- `count_data.py`, `count_records.py` — utilitas hitung jumlah record
- Output: `merged_it_jobs.csv` (11.122 baris, skema seragam)

### Batch Analysis
- `spark_analysis.py` — agregasi PySpark (job demand, salary by role, jobs by platform,
  jobs by country) → disimpan ke `spark_outputs/`

### Storage
- `import_to_mongodb.py` — import `merged_it_jobs.csv` ke MongoDB (`bigdata_jobs.it_jobs`)

### Streaming
- `docker-compose.yml` — Kafka (bitnami, KRaft mode, port 9092)
- `kafka_producer.py` — replay data ke topic `it-jobs-stream` (simulasi real-time)
- `spark_streaming.py` — konsumsi & agregasi real-time → `stream_outputs/`
- `streaming_config.py` — konfigurasi terpusat (broker, topic, path output)
- `start_streaming.ps1` — script start otomatis (Windows)

### Dashboard
- `app.py` — Streamlit, 4 tab: Charts, Finder, MongoDB, Stream

## Cara Menjalankan

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Cleaning & merge data:
   ```bash
   python clean_data.py
   ```
3. Batch analysis Spark:
   ```bash
   python spark_analysis.py
   ```
4. (Opsional) Import ke MongoDB — pastikan MongoDB lokal aktif:
   ```bash
   python import_to_mongodb.py
   ```
5. (Opsional) Real-time streaming:
   ```bash
   docker compose up -d
   python kafka_producer.py
   python spark_streaming.py
   ```
6. Jalankan dashboard:
   ```bash
   streamlit run app.py
   ```

## Catatan
- Semua dependency sudah di-pin versinya di `requirements.txt`.
- Dashboard (`app.py`) otomatis fallback ke CSV (`merged_it_jobs.csv`) jika MongoDB tidak aktif.
- Tab Stream di dashboard butuh Kafka + `spark_streaming.py` berjalan untuk menampilkan data live.
