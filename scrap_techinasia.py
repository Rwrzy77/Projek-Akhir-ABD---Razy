import requests
import time
import pandas as pd
from kafka_helper import get_kafka_producer, publish_scraped_job

# Algolia configuration
URL = "https://219wx3mpv4-dsn.algolia.net/1/indexes/*/queries"
LIMIT_PER_PAGE = 50

# Parameter API dari DevTools
params = {
    "x-algolia-agent": "Algolia for vanilla JavaScript 3.30.0;JS Helper 2.26.1",
    "x-algolia-application-id": "219WX3MPV4",
    # Ensure API key is active
    "x-algolia-api-key": "b528008a75dc1c4402bfe0d8db8b3f8e" 
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://www.techinasia.com",
    "Referer": "https://www.techinasia.com/"
}

# IT & Hybrid keywords list
it_keywords = [
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

all_jobs = []

producer = get_kafka_producer()
if producer:
    print("Kafka Producer terhubung. Data hasil scraping akan dikirim langsung ke Kafka!")
else:
    print("[WARN] Kafka Producer tidak terhubung. Data hanya akan disimpan ke CSV.")

print("=" * 60)
print("MEMULAI PENGAMBILAN DATA IT SECARA MASIF...")
print("=" * 60)

for keyword in it_keywords:
    print(f"\n---> [TARGET AKTIF] Sedang mencari keyword: '{keyword.upper()}'")
    page = 0
    
    while True:
        payload = {
            "requests": [
                {
                    # Use job_postings index
                    "indexName": "job_postings", 
                    "params": f"query={keyword}&hitsPerPage={LIMIT_PER_PAGE}&page={page}"
                }
            ]
        }

        try:
            res = requests.post(URL, params=params, headers=headers, json=payload, timeout=10)
            res.raise_for_status()
            data = res.json()
            
            hits = data['results'][0].get('hits', [])
            
            if not hits:
                print(f"     [INFO] Data habis di halaman {page}. Lanjut ke target berikutnya.")
                break
                
            print(f"     Halaman {page:2d} | Menemukan {len(hits)} lowongan...")
            
            for item in hits:
                # Extract fields safely from JSON
                job_title = item.get("title") or item.get("name") or "Tidak dicantumkan"
                
                company_data = item.get("company") or {}
                company_name = company_data.get("name", "Tidak dicantumkan")
                
                city_data = item.get("city") or {}
                location = city_data.get("country_name") or item.get("work_country_name") or "Tidak dicantumkan"
                
                job_type = item.get("job_type", {}).get("name", "Tidak dicantumkan") if isinstance(item.get("job_type"), dict) else item.get("job_type", "Tidak dicantumkan")
                
                skills = item.get("job_skills") or item.get("skills") or []
                skills_str = ", ".join([s.get("name", "") for s in skills if isinstance(s, dict)]) if skills else "Tidak dicantumkan"

                job_data = {
                    "job_title": job_title,
                    "company_name": company_name,
                    "location": location,
                    "skills_required": skills_str,
                    "experience_years_min": item.get("experience_min", 0),
                    "job_type": job_type,
                    "published_at": item.get("published_at") or "Tidak dicantumkan",
                    "source_platform": "Tech In Asia"
                }
                all_jobs.append(job_data)
                if producer:
                    publish_scraped_job(producer, job_data, "Tech In Asia")
                
            page += 1
            time.sleep(1) # Delay to prevent rate limit

        except requests.exceptions.HTTPError as e:
            if res.status_code == 403:
                print(f"     [ERROR] 403 Forbidden! API Key sepertinya sudah expired. Silakan copy yang baru dari Inspect Element.")
            else:
                print(f"     [ERROR] HTTP Error: {e}")
            break
        except Exception as e:
            print(f"     [ERROR] Terjadi kendala: {e}")
            break

# Clean and save data
if producer:
    producer.flush()
    producer.close()

if all_jobs:
    df = pd.DataFrame(all_jobs)
    # Remove duplicates
    df_clean = df.drop_duplicates(subset=['job_title', 'company_name'])
    
    file_name = "techinasia_it_massive.csv"
    df_clean.to_csv(file_name, index=False, encoding="utf-8-sig", sep=";")
    
    print("\n" + "=" * 60)
    print(f"[SUKSES BESAR] Scraping Selesai!")
    print(f"Total Data Kotor Terambil: {len(all_jobs)} baris")
    print(f"Total Data Bersih (Siap Pakai): {len(df_clean)} baris")
    print(f"File tersimpan sebagai '{file_name}'")
    print("=" * 60)
else:
    print("\n[GAGAL] Tidak ada data yang berhasil diambil.")