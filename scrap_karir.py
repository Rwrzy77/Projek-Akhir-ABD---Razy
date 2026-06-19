import requests
import pandas as pd
import time
import re
import os
import urllib3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from kafka_helper import get_kafka_producer, publish_scraped_job

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
FILE_NAME  = "karir_dataset_master.csv"
LIST_URL   = "https://gateway2-beta.karir.com/v2/search/opportunities"
DETAIL_URL = "https://gateway2-beta.karir.com/v1/opportunity/detail"

# IT & Hybrid keywords list
KEYWORDS = [
    "Software Engineer", "Backend Developer", "Frontend Developer", "Fullstack Developer",
    "Web Developer", "Mobile Developer", "Android Developer", "iOS Developer",
    "React Developer", "Vue Developer", "Angular Developer", "Node JS Developer",
    "Python Developer", "Java Developer", "Golang Developer",
    "Data Scientist", "Data Analyst", "Data Engineer", "Data Architect",
    "Machine Learning Engineer", "Artificial Intelligence Engineer", "Deep Learning Engineer",
    "Business Intelligence Developer", "Database Administrator", "Big Data Specialist",
    "DevOps Engineer", "Cloud Engineer", "Site Reliability Engineer", "System Administrator",
    "Network Engineer", "Network Administrator", "Infrastructure Engineer", "Cyber Security Analyst",
    "Information Security Specialist", "Penetration Tester", "Security Engineer",
    "Product Manager", "Project Manager IT", "Product Owner", "Scrum Master",
    "Business Analyst IT", "Systems Analyst", "IT Consultant", "Solution Architect",
    "ERP Consultant", "SAP Consultant",
    "QA Engineer", "Software Tester", "Automation Test Engineer", "Quality Assurance Analyst",
    "Manual Tester",
    "UI UX Designer", "UX Researcher", "Product Designer", "Graphic Designer",
    "Web Designer",
    "IT Support", "Technical Support Specialist", "Game Developer", "Blockchain Developer",
    "Technical SEO Specialist", "Digital Marketing Analyst", "Growth Marketer", "Web Analyst",
    "E-commerce Specialist", "E-commerce Manager", "CRM Specialist", "Salesforce Administrator",
    "HubSpot Specialist", "Product Marketing Manager",
    "Quantitative Analyst", "Financial Risk Analyst", "Supply Chain Analyst", "Operations Analyst",
    "Business Analyst", "Business Process Analyst", "Business Process Automation Specialist", "RPA Specialist",
    "Fintech Specialist", "ERP Specialist",
    "Technical Recruiter", "IT Recruiter", "Technical Sales Consultant", "Pre-Sales Consultant",
    "Solutions Consultant", "Customer Success Manager", "Technical Account Manager", "Technical Writer",
    "Documentation Specialist", "Instructional Designer",
    "Data Privacy Officer", "IT Compliance Specialist", "IT Auditor", "GIS Analyst",
    "GIS Specialist", "Digital Forensic Examiner", "Bioinformatics Analyst", "Computational Biologist",
    "Digital Specialist", "IT Procurement Specialist"
]

def get_guest_session():
    print("[SETUP] Memperoleh session baru dari Karir.com...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    token = None
    cookies = {}

    try:
        driver.get("https://karir.com/search-lowongan")
        time.sleep(5)
        
        # Get token from localStorage
        token = driver.execute_script("return window.localStorage.getItem('token');")
        if not token:
            # Fallback to search keys similar to jwt
            all_keys = driver.execute_script("return Object.keys(window.localStorage);") or []
            for k in all_keys:
                if "token" in k.lower():
                    val = driver.execute_script(f"return window.localStorage.getItem('{k}');")
                    if val and '.' in str(val):
                        token = val
                        break
        
        for c in driver.get_cookies():
            cookies[c["name"]] = c["value"]
            
    except Exception as e:
        print(f"[SETUP] Error: {e}")
    finally:
        driver.quit()
    return token, cookies

def bersihkan_teks(teks):
    if not teks or (isinstance(teks, float) and pd.isna(teks)):
        return "Tidak dicantumkan"
    teks = re.sub(r'<.*?>', ' ', str(teks))
    teks = teks.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&quot;', '"')
    return re.sub(r' {2,}', ' ', teks).strip() or "Tidak dicantumkan"

def format_salary(lower, upper):
    if lower and upper:
        return f"Rp {int(lower):,} - Rp {int(upper):,}".replace(",", ".")
    return "Tidak dicantumkan"

def build_headers(token=None):
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://karir.com/",
        "Origin": "https://karir.com",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

# Main Logic
token, session_cookies = get_guest_session()
headers = build_headers(token)

producer = get_kafka_producer()
if producer:
    print("Kafka Producer terhubung. Data hasil scraping akan dikirim langsung ke Kafka!")
else:
    print("[WARN] Kafka Producer tidak terhubung. Data hanya akan disimpan ke CSV.")

all_jobs = []
print("=" * 60)
print("Memulai scraping Karir.com (Fokus IT Keywords)")
print("=" * 60)

for keyword in KEYWORDS:
    print(f"\nSearching Keyword: '{keyword}'")
    page = 1
    limit = 20  # API limit is 20
    
    while page <= 30:
        offset = (page - 1) * limit
        payload = {
            "is_opportunity": True,
            "limit": limit,
            "offset": offset,
            "sort_order": "newest",
            "q": keyword  # Search query parameter
        }
        
        try:
            res = requests.post(LIST_URL, json=payload, headers=headers, cookies=session_cookies, verify=False, timeout=20)
            
            if res.status_code in (401, 403):
                token, session_cookies = get_guest_session()
                headers = build_headers(token)
                continue
                
            data = res.json()
            opportunities = data.get("data", {}).get("opportunities", [])
            
            if not opportunities:
                break
                
            print(f"  Hal {page}: +{len(opportunities)} job", end=" ", flush=True)
            
            for job in opportunities:
                # Filter job title if query ignored
                title = job.get("job_position", "").lower()
                if not any(k.lower() in title for k in [keyword, "it", "tech", "data", "engineer", "dev"]):
                    pass
                
                job_id = job.get("id")
                try:
                    detail_res = requests.post(DETAIL_URL, json={"opportunity_id": job_id, "language": "id"}, 
                                             headers=headers, cookies=session_cookies, verify=False, timeout=10)
                    detail_json = detail_res.json().get("data", {})
                    
                    job_data = {
                        "job_title": bersihkan_teks(job.get("job_position")),
                        "company_name": bersihkan_teks(job.get("company_name")),
                        "location": bersihkan_teks(detail_json.get("location") or job.get("location_name")),
                        "job_type": bersihkan_teks(detail_json.get("job_type")),
                        "experience_level": f"{detail_json.get('work_experience', 0)} Tahun",
                        "education_req": ", ".join(detail_json.get("degrees", [])),
                        "salary_range": format_salary(job.get("salary_lower"), job.get("salary_upper")),
                        "job_requirements": bersihkan_teks(detail_json.get("requirements")),
                        "job_responsibilities": bersihkan_teks(detail_json.get("responsibilities")),
                        "posted_date": str(job.get("posted_at", "")).split("T")[0],
                        "source_platform": "Karir.com",
                    }
                    all_jobs.append(job_data)
                    if producer:
                        publish_scraped_job(producer, job_data, "Karir.com")
                    time.sleep(0.1)
                except:
                    continue
            
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error: {e}")
            break

# Saving Data
if producer:
    producer.flush()
    producer.close()

if all_jobs:
    df_baru = pd.DataFrame(all_jobs)
    if os.path.exists(FILE_NAME):
        df_lama = pd.read_csv(FILE_NAME, sep=";")
        df_gabungan = pd.concat([df_lama, df_baru], ignore_index=True)
        df_gabungan.drop_duplicates(subset=["job_title", "company_name", "location"], keep="last", inplace=True)
    else:
        df_gabungan = df_baru
    
    df_gabungan.to_csv(FILE_NAME, index=False, encoding="utf-8-sig", sep=";")
    print(f"\nTOTAL FINAL DATA: {len(df_gabungan)} rows saved to {FILE_NAME}")
else:
    print("\nNo new data found.")