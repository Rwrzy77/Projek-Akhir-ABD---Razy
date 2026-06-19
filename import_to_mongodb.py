"""Import merged IT jobs dataset into MongoDB."""

import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING, TEXT
from datetime import datetime
import sys

# Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bigdata_jobs"
COLLECTION_NAME = "it_jobs"
CSV_FILE = "merged_it_jobs.csv"

def connect_mongodb():
    """Connect to MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test koneksi
        client.admin.command('ping')
        print(f"Berhasil terhubung ke MongoDB: {MONGO_URI}")
        return client
    except Exception as e:
        print(f"[ERROR] Gagal terhubung ke MongoDB: {e}")
        sys.exit(1)

def clean_value(val):
    """Clean NaN/None values"""
    if pd.isna(val):
        return None
    if isinstance(val, float) and np.isinf(val):
        return None
    return val

def prepare_documents(df):
    """Convert DataFrame to MongoDB documents"""
    documents = []
    
    for _, row in df.iterrows():
        doc = {
            "job_title": clean_value(row.get('job_title')),
            "company_name": clean_value(row.get('company_name')),
            "location": clean_value(row.get('location')),
            "country": clean_value(row.get('country')),
            "job_type": clean_value(row.get('job_type')),
            "experience": clean_value(row.get('experience')),
            "education": clean_value(row.get('education')),
            "salary": {
                "min": clean_value(row.get('salary_min')),
                "max": clean_value(row.get('salary_max')),
            },
            "skills": clean_value(row.get('skills')),
            "keyword": clean_value(row.get('keyword')),
            "platform": clean_value(row.get('platform')),
            "post_date": clean_value(row.get('post_date')),
            "job_url": clean_value(row.get('job_url')),
            "imported_at": datetime.now()
        }
        
        # Calculate average salary
        if doc["salary"]["min"] is not None and doc["salary"]["max"] is not None:
            doc["salary"]["avg"] = (doc["salary"]["min"] + doc["salary"]["max"]) / 2
        else:
            doc["salary"]["avg"] = None
        
        documents.append(doc)
    
    return documents

def create_indexes(collection):
    """Create MongoDB indexes"""
    print("\nMembuat indexes...")
    
    # Text index
    collection.create_index([
        ("job_title", TEXT),
        ("company_name", TEXT),
        ("skills", TEXT)
    ], name="text_search_idx")
    
    # Filters indexes
    collection.create_index([("platform", ASCENDING)], name="platform_idx")
    collection.create_index([("country", ASCENDING)], name="country_idx")
    collection.create_index([("keyword", ASCENDING)], name="keyword_idx")
    collection.create_index([("job_type", ASCENDING)], name="job_type_idx")
    collection.create_index([("salary.min", ASCENDING)], name="salary_min_idx")
    collection.create_index([("salary.max", ASCENDING)], name="salary_max_idx")
    collection.create_index([("post_date", ASCENDING)], name="post_date_idx")
    
    # Compound index
    collection.create_index([
        ("platform", ASCENDING),
        ("country", ASCENDING),
        ("keyword", ASCENDING)
    ], name="platform_country_keyword_idx")
    
    print("Indexes berhasil dibuat!")

def print_summary(collection):
    """Show MongoDB summary statistics"""
    print("\n" + "=" * 60)
    print("  RINGKASAN DATA DI MONGODB")
    print("=" * 60)
    
    total = collection.count_documents({})
    print(f"\n  Total Dokumen: {total:,}")
    
    # Platform counts
    print("\n  Per Platform:")
    pipeline = [
        {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in collection.aggregate(pipeline):
        pct = (doc['count'] / total) * 100
        print(f"     {doc['_id']:<15} : {doc['count']:>5,} ({pct:.1f}%)")
    
    # Top 5 countries
    print("\n  Top 5 Negara:")
    pipeline = [
        {"$group": {"_id": "$country", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline):
        print(f"     {doc['_id']:<20} : {doc['count']:>5,}")
    
    # Salary stats
    print("\n  Statistik Gaji:")
    with_salary = collection.count_documents({"salary.min": {"$ne": None}})
    print(f"     Dengan info gaji : {with_salary:,} ({with_salary/total*100:.1f}%)")
    
    pipeline = [
        {"$match": {"salary.avg": {"$ne": None}}},
        {"$group": {
            "_id": None,
            "avg_salary": {"$avg": "$salary.avg"},
            "min_salary": {"$min": "$salary.min"},
            "max_salary": {"$max": "$salary.max"}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        r = result[0]
        print(f"     Rata-rata gaji   : Rp {r['avg_salary']/1000000:,.2f} Juta")
        print(f"     Gaji terendah    : Rp {r['min_salary']/1000000:,.2f} Juta")
        print(f"     Gaji tertinggi   : Rp {r['max_salary']/1000000:,.2f} Juta")
    
    # Top 5 keywords
    print("\n  Top 5 Keyword:")
    pipeline = [
        {"$group": {"_id": "$keyword", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in collection.aggregate(pipeline):
        print(f"     {doc['_id']:<30} : {doc['count']:>5,}")
    
    print("\n" + "=" * 60)

def main():
    print("=" * 60)
    print("  IMPORT DATA KE MONGODB")
    print("=" * 60)
    
    # 1. Baca CSV
    print(f"\nMembaca {CSV_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE, sep=";", encoding="utf-8-sig", low_memory=False)
        print(f"   Berhasil membaca {len(df):,} baris, {len(df.columns)} kolom")
    except Exception as e:
        print(f"[ERROR] Gagal membaca CSV: {e}")
        sys.exit(1)
    
    # 2. Koneksi MongoDB
    client = connect_mongodb()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # 3. Hapus data lama (fresh import)
    old_count = collection.count_documents({})
    if old_count > 0:
        print(f"\nMenghapus {old_count:,} dokumen lama...")
        collection.delete_many({})
    
    # 4. Konversi dan import
    print("\nMengkonversi data ke format MongoDB...")
    documents = prepare_documents(df)
    print(f"   {len(documents):,} dokumen siap diimport")
    
    print("\nMengimport ke MongoDB...")
    # Batch insert
    batch_size = 500
    total_inserted = 0
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        result = collection.insert_many(batch)
        total_inserted += len(result.inserted_ids)
        print(f"   Batch {i//batch_size + 1}: {total_inserted:,}/{len(documents):,} inserted")
    
    print(f"\nBerhasil import {total_inserted:,} dokumen ke {DB_NAME}.{COLLECTION_NAME}")
    
    # 5. Buat indexes
    create_indexes(collection)
    
    # 6. Tampilkan ringkasan
    print_summary(collection)
    
    # Tutup koneksi
    client.close()
    print("\nKoneksi MongoDB ditutup.")
    print("Proses import selesai!\n")

if __name__ == "__main__":
    main()
