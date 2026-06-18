import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import numpy as np
from datetime import datetime, timezone

from streaming_config import (
    STREAM_KEYWORD_COUNTS,
    STREAM_OUTPUT_DIR,
    STREAM_PLATFORM_COUNTS,
    STREAM_RECENT_JOBS,
    STREAM_SUMMARY,
)

# ============================================================
#   MONGODB CONNECTION
# ============================================================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bigdata_jobs"
COLLECTION_NAME = "it_jobs"

def get_mongo_client():
    """Koneksi ke MongoDB, return None jika gagal"""
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        return client
    except Exception:
        return None

def load_from_mongodb():
    """Load data dari MongoDB dan konversi ke DataFrame"""
    client = get_mongo_client()
    if client is None:
        return None, None
    
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Cek apakah collection punya data
    if collection.count_documents({}) == 0:
        client.close()
        return None, None
    
    # Ambil semua dokumen
    cursor = collection.find({}, {"_id": 0, "imported_at": 0})
    docs = list(cursor)
    
    df = pd.DataFrame(docs)
    
    # Flatten salary nested document
    if 'salary' in df.columns:
        df['salary_min'] = df['salary'].apply(lambda x: x.get('min') if isinstance(x, dict) else None)
        df['salary_max'] = df['salary'].apply(lambda x: x.get('max') if isinstance(x, dict) else None)
        df['salary_avg'] = df['salary'].apply(lambda x: x.get('avg') if isinstance(x, dict) else None)
        df.drop(columns=['salary'], inplace=True)
    
    return df, collection

def mongo_aggregate(collection, pipeline):
    """Jalankan MongoDB aggregation pipeline"""
    try:
        return list(collection.aggregate(pipeline))
    except Exception:
        return []

# ============================================================
#   PAGE CONFIG & CSS
# ============================================================
st.set_page_config(
    page_title="Big Data IT Job Market Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }
    
    .stApp {
        background-color: #0f172a;
        color: #f8fafc;
    }
    
    .premium-card {
        background: rgba(30, 41, 59, 0.45);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 24px;
        backdrop-filter: blur(12px);
        margin-bottom: 20px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    .premium-card:hover {
        transform: translateY(-4px);
        border-color: rgba(99, 102, 241, 0.4);
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3), 0 8px 10px -6px rgba(99, 102, 241, 0.1);
    }
    
    .metric-value {
        font-size: 36px;
        font-weight: 800;
        background: linear-gradient(135deg, #818cf8, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 4px;
    }
    
    .metric-label {
        font-size: 14px;
        font-weight: 500;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    h1, h2, h3 {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
        color: #f8fafc !important;
    }
    
    .gradient-text {
        background: linear-gradient(135deg, #38bdf8, #818cf8, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }
    
    section[data-testid="stSidebar"] {
        background-color: #0b0f19;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    .mongo-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #00684a, #00ed64);
        color: #fff;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    
    .csv-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #6366f1, #818cf8);
        color: #fff;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    
    .spark-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #e25a1c, #ff6f00);
        color: #fff;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    
    .kafka-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #1f2937, #4b5563);
        color: #fff;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.02em;
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    .status-bar {
        display: flex;
        gap: 12px;
        justify-content: center;
        margin-bottom: 24px;
        flex-wrap: wrap;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
#   DATA LOADING
# ============================================================
@st.cache_data(ttl=300)
def load_data():
    """Load data: prioritas MongoDB > CSV"""
    source = "csv"
    collection_ref = None
    
    # Coba MongoDB dulu
    df_mongo, coll = load_from_mongodb()
    if df_mongo is not None and not df_mongo.empty:
        source = "mongodb"
        return df_mongo, source
    
    # Fallback ke CSV
    if os.path.exists("merged_it_jobs.csv"):
        df = pd.read_csv("merged_it_jobs.csv", delimiter=";", encoding="utf-8-sig", low_memory=False)
        return df, source
    
    return pd.DataFrame(), source

@st.cache_data
def load_spark_outputs():
    spark_data = {}
    if os.path.exists("spark_outputs/job_demand.csv"):
        spark_data['job_demand'] = pd.read_csv("spark_outputs/job_demand.csv", delimiter=";")
    if os.path.exists("spark_outputs/salary_by_role.csv"):
        spark_data['salary_by_role'] = pd.read_csv("spark_outputs/salary_by_role.csv", delimiter=";")
    if os.path.exists("spark_outputs/jobs_by_platform.csv"):
        spark_data['jobs_by_platform'] = pd.read_csv("spark_outputs/jobs_by_platform.csv", delimiter=";")
    if os.path.exists("spark_outputs/jobs_by_country.csv"):
        spark_data['jobs_by_country'] = pd.read_csv("spark_outputs/jobs_by_country.csv", delimiter=";")
    return spark_data

@st.cache_data(ttl=10)
def load_stream_outputs():
    """Load real-time streaming outputs dari Spark Structured Streaming."""
    stream_data = {}
    summary = None

    if os.path.exists(STREAM_SUMMARY):
        try:
            with open(STREAM_SUMMARY, encoding="utf-8") as f:
                summary = json.load(f)
        except (json.JSONDecodeError, OSError):
            summary = None

    if os.path.exists(STREAM_PLATFORM_COUNTS):
        stream_data["platform_counts"] = pd.read_csv(STREAM_PLATFORM_COUNTS, delimiter=";")
    if os.path.exists(STREAM_KEYWORD_COUNTS):
        stream_data["keyword_counts"] = pd.read_csv(STREAM_KEYWORD_COUNTS, delimiter=";")
    if os.path.exists(STREAM_RECENT_JOBS):
        stream_data["recent_jobs"] = pd.read_csv(STREAM_RECENT_JOBS, delimiter=";")

    windowed_path = os.path.join(STREAM_OUTPUT_DIR, "live_windowed_platform.csv")
    if os.path.exists(windowed_path):
        stream_data["windowed_platform"] = pd.read_csv(windowed_path, delimiter=";")

    return stream_data, summary

def is_stream_active(summary):
    """Stream dianggap aktif jika summary diupdate < 2 menit terakhir."""
    if not summary or "last_updated" not in summary:
        return False
    try:
        updated = datetime.fromisoformat(summary["last_updated"].replace("Z", "+00:00"))
        age_sec = (datetime.now(timezone.utc) - updated).total_seconds()
        return age_sec < 120
    except (ValueError, TypeError):
        return False

# ============================================================
#   MONGODB AGGREGATION HELPERS
# ============================================================
def mongo_get_job_demand(collection, limit=15):
    pipeline = [
        {"$group": {"_id": "$keyword", "total_jobs": {"$sum": 1}}},
        {"$sort": {"total_jobs": -1}},
        {"$limit": limit},
        {"$project": {"keyword": "$_id", "total_jobs": 1, "_id": 0}}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_get_salary_by_role(collection, limit=10):
    pipeline = [
        {"$match": {"salary.min": {"$ne": None}, "salary.max": {"$ne": None}}},
        {"$group": {
            "_id": "$keyword",
            "total_jobs_with_salary": {"$sum": 1},
            "avg_salary_min": {"$avg": "$salary.min"},
            "avg_salary_max": {"$avg": "$salary.max"},
            "avg_salary_overall": {"$avg": "$salary.avg"}
        }},
        {"$sort": {"avg_salary_overall": -1}},
        {"$limit": limit},
        {"$project": {
            "keyword": "$_id", "_id": 0,
            "total_jobs_with_salary": 1,
            "avg_salary_min": {"$round": ["$avg_salary_min", 2]},
            "avg_salary_max": {"$round": ["$avg_salary_max", 2]},
            "avg_salary_overall": {"$round": ["$avg_salary_overall", 2]}
        }}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_get_by_platform(collection):
    pipeline = [
        {"$group": {"_id": "$platform", "total_jobs": {"$sum": 1}}},
        {"$sort": {"total_jobs": -1}},
        {"$project": {"platform": "$_id", "total_jobs": 1, "_id": 0}}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_get_by_country(collection, limit=10):
    pipeline = [
        {"$group": {"_id": "$country", "total_jobs": {"$sum": 1}}},
        {"$sort": {"total_jobs": -1}},
        {"$limit": limit},
        {"$project": {"country": "$_id", "total_jobs": 1, "_id": 0}}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_get_by_job_type(collection):
    pipeline = [
        {"$group": {"_id": "$job_type", "total_jobs": {"$sum": 1}}},
        {"$sort": {"total_jobs": -1}},
        {"$project": {"job_type": "$_id", "total_jobs": 1, "_id": 0}}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_get_top_companies(collection, limit=10):
    pipeline = [
        {"$match": {"company_name": {"$ne": None}}},
        {"$group": {"_id": "$company_name", "total_jobs": {"$sum": 1}}},
        {"$sort": {"total_jobs": -1}},
        {"$limit": limit},
        {"$project": {"company_name": "$_id", "total_jobs": 1, "_id": 0}}
    ]
    return pd.DataFrame(mongo_aggregate(collection, pipeline))

def mongo_search_jobs(collection, query=None, platforms=None, countries=None, min_salary=0):
    """Search jobs with MongoDB queries"""
    match_filter = {}
    
    if platforms:
        match_filter["platform"] = {"$in": platforms}
    if countries:
        match_filter["country"] = {"$in": countries}
    if min_salary > 0:
        match_filter["$or"] = [
            {"salary.min": {"$gte": min_salary}},
            {"salary.max": {"$gte": min_salary}}
        ]
    if query:
        match_filter["$text"] = {"$search": query}
    
    cursor = collection.find(
        match_filter,
        {"_id": 0, "imported_at": 0}
    )
    
    docs = list(cursor)
    if not docs:
        return pd.DataFrame()
    
    df = pd.DataFrame(docs)
    if 'salary' in df.columns:
        df['salary_min'] = df['salary'].apply(lambda x: x.get('min') if isinstance(x, dict) else None)
        df['salary_max'] = df['salary'].apply(lambda x: x.get('max') if isinstance(x, dict) else None)
        df.drop(columns=['salary'], inplace=True)
    return df

# ============================================================
#   MAIN APP
# ============================================================

# Header
st.markdown("<h1 style='text-align: center; margin-bottom: 5px;'>📊 <span class='gradient-text'>Big Data IT Job Market Dashboard</span></h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #94a3b8; font-size: 16px; margin-bottom: 10px;'>Analisis Terintegrasi Lowongan Kerja IT dari 5 Platform: Glints, LinkedIn, Indeed, Karir.com & Tech in Asia</p>", unsafe_allow_html=True)

# Load data
df_raw, data_source = load_data()
spark_data = load_spark_outputs()
stream_data, stream_summary = load_stream_outputs()
has_spark_output = len(spark_data) == 4
has_stream_output = len(stream_data) > 0
stream_is_live = is_stream_active(stream_summary)

# Get MongoDB collection for aggregation queries
mongo_client = get_mongo_client()
mongo_coll = None
if mongo_client:
    mongo_coll = mongo_client[DB_NAME][COLLECTION_NAME]

if df_raw.empty:
    st.error("Data tidak ditemukan. Jalankan `clean_data.py` lalu `import_to_mongodb.py` terlebih dahulu.")
    st.stop()

# Status badges
badges_html = '<div class="status-bar">'
if data_source == "mongodb":
    badges_html += '<span class="mongo-badge">🍃 MongoDB Connected</span>'
else:
    badges_html += '<span class="csv-badge">📄 CSV Mode</span>'
if has_spark_output:
    badges_html += '<span class="spark-badge">⚡ Spark Data Ready</span>'
if stream_is_live:
    badges_html += '<span class="kafka-badge">📡 Kafka Stream LIVE</span>'
elif has_stream_output:
    badges_html += '<span class="kafka-badge">📡 Stream Data Available</span>'
badges_html += '</div>'
st.markdown(badges_html, unsafe_allow_html=True)

# Sidebar
st.sidebar.markdown("<h3 style='margin-top: 0;'>⚙️ Filter & Navigasi</h3>", unsafe_allow_html=True)

if data_source == "mongodb":
    st.sidebar.success("🍃 Data dari **MongoDB**")
    st.sidebar.caption(f"Database: `{DB_NAME}`\nCollection: `{COLLECTION_NAME}`")
else:
    st.sidebar.info("📄 Data dari **CSV** (MongoDB tidak tersedia)")

st.sidebar.markdown("---")
st.sidebar.caption(f"Total records: **{len(df_raw):,}**")
st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ============================================================
#   METRICS ROW
# ============================================================
col1, col2, col3, col4 = st.columns(4)

total_jobs = len(df_raw)

if 'salary_min' in df_raw.columns:
    jobs_with_salary = df_raw['salary_min'].notna().sum()
else:
    jobs_with_salary = 0
salary_pct = (jobs_with_salary / total_jobs) * 100 if total_jobs > 0 else 0

# Average salary
if 'salary_min' in df_raw.columns and 'salary_max' in df_raw.columns:
    df_valid_salary = df_raw.dropna(subset=['salary_min', 'salary_max'])
    if not df_valid_salary.empty:
        avg_sal_overall = (df_valid_salary['salary_min'].mean() + df_valid_salary['salary_max'].mean()) / 2
        avg_sal_str = f"Rp {avg_sal_overall/1000000:.2f} Jt"
    else:
        avg_sal_str = "N/A"
else:
    avg_sal_str = "N/A"

if 'platform' in df_raw.columns and not df_raw['platform'].empty:
    top_platform = df_raw['platform'].value_counts().idxmax()
    top_platform_count = df_raw['platform'].value_counts().max()
else:
    top_platform = "N/A"
    top_platform_count = 0

with col1:
    st.markdown(f"""
    <div class="premium-card">
        <div class="metric-label">Total Lowongan Kerja</div>
        <div class="metric-value">{total_jobs:,}</div>
        <div style="font-size: 12px; color: #10b981;">🟢 5 Platform Terintegrasi</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="premium-card">
        <div class="metric-label">Lowongan Dengan Gaji</div>
        <div class="metric-value">{jobs_with_salary:,}</div>
        <div style="font-size: 12px; color: #6366f1;">{salary_pct:.1f}% dari total loker</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="premium-card">
        <div class="metric-label">Rata-rata Gaji Bulanan</div>
        <div class="metric-value">{avg_sal_str}</div>
        <div style="font-size: 12px; color: #a855f7;">Berdasarkan IDR rate</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="premium-card">
        <div class="metric-label">Platform Terbanyak</div>
        <div class="metric-value">{top_platform}</div>
        <div style="font-size: 12px; color: #38bdf8;">{top_platform_count:,} Loker ({top_platform_count/total_jobs*100:.1f}%)</div>
    </div>
    """, unsafe_allow_html=True)

# ============================================================
#   TABS
# ============================================================
tab_charts, tab_finder, tab_mongo, tab_stream = st.tabs([
    "📈 Analisis Tren & Visualisasi",
    "🔍 Interactive Job Finder",
    "🍃 MongoDB Analytics",
    "📡 Real-time Streaming"
])

# ============================================================
#   TAB 1: ANALISIS TREN
# ============================================================
with tab_charts:
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("🔥 Tren Kategori / Keyword Pekerjaan IT")
        
        if has_spark_output:
            df_demand = spark_data['job_demand'].head(15)
            st.caption("⚡ Diproses menggunakan Spark Session")
        elif mongo_coll and data_source == "mongodb":
            df_demand = mongo_get_job_demand(mongo_coll, 15)
            st.caption("🍃 Diproses menggunakan MongoDB Aggregation")
        else:
            df_demand = df_raw['keyword'].value_counts().reset_index().head(15)
            df_demand.columns = ['keyword', 'total_jobs']
            st.caption("ℹ️ Diproses menggunakan Pandas fallback")
            
        if not df_demand.empty:
            fig_demand = px.bar(
                df_demand,
                x='total_jobs',
                y='keyword',
                orientation='h',
                labels={'total_jobs': 'Jumlah Lowongan', 'keyword': 'Role / Keyword'},
                color='total_jobs',
                color_continuous_scale=px.colors.sequential.Sunsetdark
            )
            fig_demand.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc',
                yaxis={'categoryorder': 'total ascending'}
            )
            st.plotly_chart(fig_demand, use_container_width=True)

        st.subheader("🌐 Distribusi Loker Berdasarkan Negara")
        if has_spark_output:
            df_country = spark_data['jobs_by_country'].head(10)
        elif mongo_coll and data_source == "mongodb":
            df_country = mongo_get_by_country(mongo_coll, 10)
        else:
            df_country = df_raw['country'].value_counts().reset_index().head(10)
            df_country.columns = ['country', 'total_jobs']
            
        if not df_country.empty:
            fig_country = px.pie(
                df_country,
                values='total_jobs',
                names='country',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_country.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc'
            )
            st.plotly_chart(fig_country, use_container_width=True)

    with col_right:
        st.subheader("💰 Rata-rata Gaji Berdasarkan Role IT")
        if has_spark_output:
            df_sal = spark_data['salary_by_role'].head(10)
        elif mongo_coll and data_source == "mongodb":
            df_sal = mongo_get_salary_by_role(mongo_coll, 10)
        else:
            df_sal_valid = df_raw.dropna(subset=['salary_min', 'salary_max'])
            if not df_sal_valid.empty:
                df_sal = df_sal_valid.groupby('keyword').agg(
                    avg_salary_min=('salary_min', 'mean'),
                    avg_salary_max=('salary_max', 'mean'),
                    avg_salary_overall=('salary_min', lambda x: (x.mean() + df_sal_valid.loc[x.index, 'salary_max'].mean()) / 2)
                ).reset_index().sort_values('avg_salary_overall', ascending=False).head(10)
            else:
                df_sal = pd.DataFrame()
            
        if not df_sal.empty:
            fig_sal = go.Figure()
            fig_sal.add_trace(go.Bar(
                y=df_sal['keyword'],
                x=df_sal['avg_salary_min'],
                name='Gaji Minimum',
                orientation='h',
                marker_color='#818cf8'
            ))
            fig_sal.add_trace(go.Bar(
                y=df_sal['keyword'],
                x=df_sal['avg_salary_max'],
                name='Gaji Maksimum',
                orientation='h',
                marker_color='#c084fc'
            ))
            fig_sal.update_layout(
                barmode='group',
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc',
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title='Rupiah (IDR)'
            )
            st.plotly_chart(fig_sal, use_container_width=True)
        else:
            st.info("Tidak ada data gaji yang tersedia.")
        
        st.subheader("📊 Distribusi Lowongan per Platform")
        if has_spark_output:
            df_plat = spark_data['jobs_by_platform']
        elif mongo_coll and data_source == "mongodb":
            df_plat = mongo_get_by_platform(mongo_coll)
        else:
            df_plat = df_raw['platform'].value_counts().reset_index()
            df_plat.columns = ['platform', 'total_jobs']
            
        color_map = {
            'LinkedIn': '#0a66c2',
            'Glints': '#10b981',
            'Tech In Asia': '#f97316',
            'Karir.com': '#8b5cf6',
            'Indeed': '#2557a7'
        }
        
        if not df_plat.empty:
            fig_plat = px.bar(
                df_plat,
                x='platform',
                y='total_jobs',
                color='platform',
                color_discrete_map=color_map,
                labels={'platform': 'Platform', 'total_jobs': 'Jumlah Loker'}
            )
            fig_plat.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc'
            )
            st.plotly_chart(fig_plat, use_container_width=True)

# ============================================================
#   TAB 2: JOB FINDER
# ============================================================
with tab_finder:
    st.subheader("🔍 Pencarian & Filter Database Lowongan Kerja IT")
    
    use_mongo_search = data_source == "mongodb" and mongo_coll is not None
    
    if use_mongo_search:
        st.caption("🍃 Pencarian menggunakan MongoDB Text Search & Indexed Queries")
    
    search_query = st.text_input("Cari kata kunci posisi atau nama perusahaan:", "", key="job_search")
    
    f_col1, f_col2, f_col3 = st.columns(3)
    
    with f_col1:
        platform_options = sorted(df_raw['platform'].dropna().unique().tolist())
        selected_platforms = st.multiselect(
            "Filter Platform:",
            options=platform_options,
            default=platform_options,
            key="plat_filter"
        )
        
    with f_col2:
        country_options = sorted(df_raw['country'].dropna().unique().tolist())
        selected_countries = st.multiselect(
            "Filter Negara:",
            options=country_options,
            default=country_options,
            key="country_filter"
        )
        
    with f_col3:
        min_salary_opt = st.slider(
            "Tampilkan gaji minimal di atas (Juta Rupiah):",
            min_value=0,
            max_value=100,
            value=0,
            step=1,
            key="sal_filter"
        ) * 1000000

    # Perform search
    if use_mongo_search:
        df_filtered = mongo_search_jobs(
            mongo_coll,
            query=search_query if search_query else None,
            platforms=selected_platforms if len(selected_platforms) < len(platform_options) else None,
            countries=selected_countries if len(selected_countries) < len(country_options) else None,
            min_salary=min_salary_opt
        )
    else:
        df_filtered = df_raw[
            (df_raw['platform'].isin(selected_platforms)) &
            (df_raw['country'].isin(selected_countries))
        ]
        
        if search_query:
            df_filtered = df_filtered[
                df_filtered['job_title'].str.contains(search_query, case=False, na=False) |
                df_filtered['company_name'].str.contains(search_query, case=False, na=False)
            ]
            
        if min_salary_opt > 0:
            df_filtered = df_filtered[
                (df_filtered['salary_min'] >= min_salary_opt) | 
                (df_filtered['salary_max'] >= min_salary_opt)
            ]
        
    total_filtered = len(df_filtered)
    st.markdown(f"**Menampilkan {total_filtered:,} lowongan yang cocok**")
    
    if not df_filtered.empty:
        # Pagination: 500 per halaman
        PAGE_SIZE = 500
        total_pages = max(1, (total_filtered + PAGE_SIZE - 1) // PAGE_SIZE)
        
        pg_col1, pg_col2, pg_col3 = st.columns([1, 2, 1])
        with pg_col2:
            current_page = st.number_input(
                f"Halaman (1 - {total_pages})",
                min_value=1,
                max_value=total_pages,
                value=1,
                step=1,
                key="page_selector"
            )
        
        start_idx = (current_page - 1) * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, total_filtered)
        
        st.caption(f"Menampilkan data ke **{start_idx + 1:,}** - **{end_idx:,}** dari **{total_filtered:,}** | Halaman **{current_page}** / **{total_pages}**")
        
        df_page = df_filtered.iloc[start_idx:end_idx]
        
        display_cols = ['job_title', 'company_name', 'location', 'country', 'job_type', 
                        'experience', 'education', 'salary_min', 'salary_max', 'platform', 'post_date']
        available_cols = [c for c in display_cols if c in df_page.columns]
        df_display = df_page[available_cols].copy()
        
        def format_sal(val):
            if pd.isna(val):
                return "-"
            try:
                return f"Rp {float(val)/1000000:.1f} Jt"
            except (ValueError, TypeError):
                return "-"
            
        if 'salary_min' in df_display.columns:
            df_display['salary_min'] = df_display['salary_min'].apply(format_sal)
        if 'salary_max' in df_display.columns:
            df_display['salary_max'] = df_display['salary_max'].apply(format_sal)
        
        col_config = {
            "job_title": "Posisi",
            "company_name": "Perusahaan",
            "location": "Lokasi",
            "country": "Negara",
            "job_type": "Tipe",
            "experience": "Pengalaman",
            "education": "Pendidikan",
            "salary_min": "Gaji Min",
            "salary_max": "Gaji Max",
            "platform": "Platform",
            "post_date": "Tanggal Post"
        }
        
        st.dataframe(
            df_display, 
            use_container_width=True,
            column_config={k: v for k, v in col_config.items() if k in available_cols}
        )
    else:
        st.warning("Tidak ada lowongan yang cocok dengan filter.")

# ============================================================
#   TAB 3: MONGODB ANALYTICS
# ============================================================
with tab_mongo:
    if mongo_coll is None:
        st.warning("🔌 MongoDB tidak terhubung. Pastikan MongoDB berjalan di `localhost:27017`.")
        st.info("Jalankan `python import_to_mongodb.py` untuk import data ke MongoDB.")
    else:
        st.subheader("🍃 MongoDB Aggregation Analytics")
        st.caption("Semua analitik di tab ini dijalankan langsung via MongoDB Aggregation Pipeline")
        
        m_col1, m_col2 = st.columns(2)
        
        with m_col1:
            # Top Companies
            st.markdown("#### 🏢 Top 10 Perusahaan (Paling Banyak Merekrut)")
            df_companies = mongo_get_top_companies(mongo_coll, 10)
            if not df_companies.empty:
                fig_comp = px.bar(
                    df_companies,
                    x='total_jobs',
                    y='company_name',
                    orientation='h',
                    color='total_jobs',
                    color_continuous_scale='Tealgrn',
                    labels={'total_jobs': 'Jumlah Loker', 'company_name': 'Perusahaan'}
                )
                fig_comp.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc',
                    yaxis={'categoryorder': 'total ascending'},
                    showlegend=False
                )
                st.plotly_chart(fig_comp, use_container_width=True)
            
            # Job Type Distribution
            st.markdown("#### 📋 Distribusi Tipe Pekerjaan")
            df_jtype = mongo_get_by_job_type(mongo_coll)
            if not df_jtype.empty:
                fig_jtype = px.pie(
                    df_jtype,
                    values='total_jobs',
                    names='job_type',
                    hole=0.45,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig_jtype.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc'
                )
                st.plotly_chart(fig_jtype, use_container_width=True)

        with m_col2:
            # Platform vs Country Heatmap
            st.markdown("#### 🗺️ Platform vs Negara (Heatmap)")
            pipeline_heatmap = [
                {"$group": {
                    "_id": {"platform": "$platform", "country": "$country"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 50}
            ]
            heatmap_data = mongo_aggregate(mongo_coll, pipeline_heatmap)
            if heatmap_data:
                df_heat = pd.DataFrame([
                    {"platform": d["_id"]["platform"], "country": d["_id"]["country"], "count": d["count"]}
                    for d in heatmap_data
                ])
                df_pivot = df_heat.pivot_table(index='country', columns='platform', values='count', fill_value=0)
                # Sort by total
                df_pivot['_total'] = df_pivot.sum(axis=1)
                df_pivot = df_pivot.sort_values('_total', ascending=False).head(10).drop(columns='_total')
                
                fig_heat = px.imshow(
                    df_pivot,
                    color_continuous_scale='Viridis',
                    labels=dict(x="Platform", y="Negara", color="Jumlah"),
                    aspect='auto'
                )
                fig_heat.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc'
                )
                st.plotly_chart(fig_heat, use_container_width=True)
            
            # Salary Range per Platform
            st.markdown("#### 💰 Rata-rata Gaji per Platform")
            pipeline_sal_plat = [
                {"$match": {"salary.avg": {"$ne": None}}},
                {"$group": {
                    "_id": "$platform",
                    "avg_min": {"$avg": "$salary.min"},
                    "avg_max": {"$avg": "$salary.max"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"avg_max": -1}}
            ]
            sal_plat_data = mongo_aggregate(mongo_coll, pipeline_sal_plat)
            if sal_plat_data:
                df_sal_plat = pd.DataFrame([
                    {
                        "platform": d["_id"],
                        "Gaji Min (Avg)": d["avg_min"],
                        "Gaji Max (Avg)": d["avg_max"],
                        "count": d["count"]
                    }
                    for d in sal_plat_data
                ])
                
                fig_sal_p = go.Figure()
                fig_sal_p.add_trace(go.Bar(
                    x=df_sal_plat['platform'],
                    y=df_sal_plat['Gaji Min (Avg)'],
                    name='Avg Min Salary',
                    marker_color='#818cf8'
                ))
                fig_sal_p.add_trace(go.Bar(
                    x=df_sal_plat['platform'],
                    y=df_sal_plat['Gaji Max (Avg)'],
                    name='Avg Max Salary',
                    marker_color='#c084fc'
                ))
                fig_sal_p.update_layout(
                    barmode='group',
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc',
                    yaxis_title='Rupiah (IDR)'
                )
                st.plotly_chart(fig_sal_p, use_container_width=True)
            else:
                st.info("Tidak ada data gaji yang tersedia per platform.")
        
        # MongoDB Query Explorer
        st.markdown("---")
        st.subheader("🔧 MongoDB Query Explorer")
        st.caption("Jalankan custom aggregation pipeline langsung di dashboard")
        
        query_preset = st.selectbox("Pilih preset query:", [
            "Custom (tulis sendiri)",
            "Top 20 Keyword terpopuler",
            "Lokasi dengan gaji tertinggi",
            "Perusahaan dengan lowongan terbanyak per negara",
            "Distribusi pengalaman yang diminta"
        ])
        
        if query_preset == "Top 20 Keyword terpopuler":
            preset_pipeline = [
                {"$group": {"_id": "$keyword", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 20}
            ]
        elif query_preset == "Lokasi dengan gaji tertinggi":
            preset_pipeline = [
                {"$match": {"salary.avg": {"$ne": None}}},
                {"$group": {
                    "_id": "$location",
                    "avg_salary": {"$avg": "$salary.avg"},
                    "job_count": {"$sum": 1}
                }},
                {"$match": {"job_count": {"$gte": 2}}},
                {"$sort": {"avg_salary": -1}},
                {"$limit": 15}
            ]
        elif query_preset == "Perusahaan dengan lowongan terbanyak per negara":
            preset_pipeline = [
                {"$group": {
                    "_id": {"country": "$country", "company": "$company_name"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$group": {
                    "_id": "$_id.country",
                    "top_company": {"$first": "$_id.company"},
                    "job_count": {"$first": "$count"}
                }},
                {"$sort": {"job_count": -1}},
                {"$limit": 10}
            ]
        elif query_preset == "Distribusi pengalaman yang diminta":
            preset_pipeline = [
                {"$group": {"_id": "$experience", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 15}
            ]
        else:
            preset_pipeline = None
        
        if preset_pipeline:
            st.code(str(preset_pipeline), language="python")
            results = mongo_aggregate(mongo_coll, preset_pipeline)
            if results:
                st.dataframe(pd.DataFrame(results), use_container_width=True)
            else:
                st.info("Query tidak mengembalikan hasil.")
        else:
            st.info("Pilih salah satu preset query di atas, atau gunakan MongoDB Compass/Shell untuk custom queries.")

# ============================================================
#   TAB 4: REAL-TIME STREAMING (Kafka + Spark Streaming)
# ============================================================
with tab_stream:
    st.subheader("📡 Real-time Streaming Pipeline")
    st.caption("Kafka → Spark Structured Streaming → Dashboard (auto-refresh setiap 10 detik)")

    if stream_is_live:
        st.success("🟢 Stream aktif — data sedang diproses secara real-time")
    elif has_stream_output:
        st.warning("🟡 Data stream tersedia, tetapi tidak ada update baru dalam 2 menit terakhir")
    else:
        st.info("Pipeline streaming belum berjalan. Ikuti langkah di bawah untuk memulai.")

    s_col1, s_col2, s_col3 = st.columns(3)
    with s_col1:
        st.markdown("""
        <div class="premium-card">
            <div class="metric-label">Kafka Topic</div>
            <div class="metric-value" style="font-size: 22px;">it-jobs-stream</div>
            <div style="font-size: 12px; color: #94a3b8;">localhost:9092</div>
        </div>
        """, unsafe_allow_html=True)
    with s_col2:
        stream_total = 0
        if "platform_counts" in stream_data:
            stream_total = int(stream_data["platform_counts"]["stream_count"].sum())
        st.markdown(f"""
        <div class="premium-card">
            <div class="metric-label">Event Diproses (Stream)</div>
            <div class="metric-value">{stream_total:,}</div>
            <div style="font-size: 12px; color: #94a3b8;">Dari sesi streaming aktif</div>
        </div>
        """, unsafe_allow_html=True)
    with s_col3:
        last_upd = "Belum ada"
        if stream_summary and "last_updated" in stream_summary:
            try:
                upd = datetime.fromisoformat(stream_summary["last_updated"].replace("Z", "+00:00"))
                last_upd = upd.strftime("%H:%M:%S UTC")
            except (ValueError, TypeError):
                last_upd = stream_summary["last_updated"]
        st.markdown(f"""
        <div class="premium-card">
            <div class="metric-label">Terakhir Diupdate</div>
            <div class="metric-value" style="font-size: 22px;">{last_upd}</div>
            <div style="font-size: 12px; color: #94a3b8;">Spark foreachBatch</div>
        </div>
        """, unsafe_allow_html=True)

    if not has_stream_output:
        st.markdown("#### 🚀 Cara Menjalankan Pipeline Streaming")
        st.code("""# Terminal 1 — Start Kafka
docker compose up -d

# Terminal 2 — Start Spark Streaming consumer
python spark_streaming.py

# Terminal 3 — Publish events ke Kafka (simulasi real-time)
python kafka_producer.py --delay 0.05

# Terminal 4 — Dashboard
streamlit run app.py""", language="bash")
        st.caption("Producer membaca `merged_it_jobs.csv` dan mengirim event satu per satu ke Kafka. Spark Streaming memproses dan menulis hasil ke `stream_outputs/`.")
    else:
        chart_col, table_col = st.columns(2)

        with chart_col:
            if "platform_counts" in stream_data and not stream_data["platform_counts"].empty:
                st.markdown("#### 📊 Event per Platform (Live)")
                df_plat_stream = stream_data["platform_counts"]
                fig_stream_plat = px.bar(
                    df_plat_stream,
                    x="platform",
                    y="stream_count",
                    color="platform",
                    labels={"platform": "Platform", "stream_count": "Jumlah Event"},
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig_stream_plat.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#f8fafc",
                    showlegend=False
                )
                st.plotly_chart(fig_stream_plat, use_container_width=True)

            if "keyword_counts" in stream_data and not stream_data["keyword_counts"].empty:
                st.markdown("#### 🔑 Top Keyword (Live Stream)")
                df_kw_stream = stream_data["keyword_counts"].head(10)
                fig_stream_kw = px.bar(
                    df_kw_stream,
                    x="stream_count",
                    y="keyword",
                    orientation="h",
                    color="stream_count",
                    color_continuous_scale="Viridis",
                    labels={"keyword": "Keyword", "stream_count": "Event"}
                )
                fig_stream_kw.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#f8fafc",
                    yaxis={"categoryorder": "total ascending"},
                    showlegend=False
                )
                st.plotly_chart(fig_stream_kw, use_container_width=True)

        with table_col:
            if "recent_jobs" in stream_data and not stream_data["recent_jobs"].empty:
                st.markdown("#### 🆕 Lowongan Terbaru (Stream)")
                df_recent = stream_data["recent_jobs"].copy()
                if "salary_min" in df_recent.columns:
                    def fmt_sal_stream(val):
                        if pd.isna(val):
                            return "-"
                        try:
                            return f"Rp {float(val)/1000000:.1f} Jt"
                        except (ValueError, TypeError):
                            return "-"
                    df_recent["salary_min"] = df_recent["salary_min"].apply(fmt_sal_stream)
                    df_recent["salary_max"] = df_recent["salary_max"].apply(fmt_sal_stream)
                st.dataframe(
                    df_recent[["event_time", "job_title", "company_name", "platform", "country", "salary_min"]],
                    use_container_width=True,
                    column_config={
                        "event_time": "Waktu Event",
                        "job_title": "Posisi",
                        "company_name": "Perusahaan",
                        "platform": "Platform",
                        "country": "Negara",
                        "salary_min": "Gaji Min",
                    }
                )

        if st.button("🔄 Refresh Stream Data", key="refresh_stream"):
            st.cache_data.clear()
            st.rerun()

        with st.expander("ℹ️ Arsitektur Pipeline"):
            st.markdown("""
```
Scraper / CSV  →  kafka_producer.py  →  Kafka (it-jobs-stream)
                                              ↓
                                    spark_streaming.py
                                    (Structured Streaming)
                                              ↓
                                    stream_outputs/*.csv
                                              ↓
                                    app.py (tab ini)
```
            """)

# Footer
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #475569; font-size: 13px;'>"
    "Big Data IT Job Market Analysis | Kafka + Spark Streaming + MongoDB + PySpark + Streamlit | "
    f"Data terakhir dimuat: {datetime.now().strftime('%d %B %Y, %H:%M WIB')}"
    "</p>",
    unsafe_allow_html=True
)

# Cleanup MongoDB connection
if mongo_client:
    mongo_client.close()
