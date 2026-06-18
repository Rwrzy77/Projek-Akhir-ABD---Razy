import csv
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# KONFIGURASI - FOKUS TEKNOLOGI & INFORMASI (IT)
# ============================================================
# Daftar keyword IT & Hybrid untuk mendapatkan lebih banyak variasi data
KEYWORDS = [
    # --- Software Development & Engineering (15) ---
    "Software Engineer", "Backend Developer", "Frontend Developer", "Fullstack Developer",
    "Web Developer", "Mobile Developer", "Android Developer", "iOS Developer",
    "React Developer", "Vue Developer", "Angular Developer", "Node JS Developer",
    "Python Developer", "Java Developer", "Golang Developer",
    
    # --- Data & AI (10) ---
    "Data Scientist", "Data Analyst", "Data Engineer", "Data Architect",
    "Machine Learning Engineer", "Artificial Intelligence Engineer", "Deep Learning Engineer",
    "Business Intelligence Developer", "Database Administrator", "Big Data Specialist",

    # --- Infrastructure, Cloud, & Security (11) ---
    "DevOps Engineer", "Cloud Engineer", "Site Reliability Engineer", "System Administrator",
    "Network Engineer", "Network Administrator", "Infrastructure Engineer", "Cyber Security Analyst",
    "Information Security Specialist", "Penetration Tester", "Security Engineer",

    # --- Product, Management, & Analysis (10) ---
    "Product Manager", "Project Manager IT", "Product Owner", "Scrum Master",
    "Business Analyst IT", "Systems Analyst", "IT Consultant", "Solution Architect",
    "ERP Consultant", "SAP Consultant",

    # --- QA & Testing (5) ---
    "QA Engineer", "Software Tester", "Automation Test Engineer", "Quality Assurance Analyst",
    "Manual Tester",

    # --- Design & Creative (5) ---
    "UI UX Designer", "UX Researcher", "Product Designer", "Graphic Designer",
    "Web Designer",

    # --- Support & Tech Specialized (4) ---
    "IT Support", "Technical Support Specialist", "Game Developer", "Blockchain Developer",

    # --- Marketing, Web, & Optimasi Digital (10) ---
    "Technical SEO Specialist", "Digital Marketing Analyst", "Growth Marketer", "Web Analyst",
    "E-commerce Specialist", "E-commerce Manager", "CRM Specialist", "Salesforce Administrator",
    "HubSpot Specialist", "Product Marketing Manager",

    # --- Analisis Bisnis, Operasi, & Keuangan (10) ---
    "Quantitative Analyst", "Financial Risk Analyst", "Supply Chain Analyst", "Operations Analyst",
    "Business Analyst", "Business Process Analyst", "Business Process Automation Specialist", "RPA Specialist",
    "Fintech Specialist", "ERP Specialist",

    # --- Penjualan Teknis, Hubungan Klien, & HR (10) ---
    "Technical Recruiter", "IT Recruiter", "Technical Sales Consultant", "Pre-Sales Consultant",
    "Solutions Consultant", "Customer Success Manager", "Technical Account Manager", "Technical Writer",
    "Documentation Specialist", "Instructional Designer",

    # --- Hukum, Audit, Keamanan, & Sains Hybrid (10) ---
    "Data Privacy Officer", "IT Compliance Specialist", "IT Auditor", "GIS Analyst",
    "GIS Specialist", "Digital Forensic Examiner", "Bioinformatics Analyst", "Computational Biologist",
    "Digital Specialist", "IT Procurement Specialist"
]

NAMA_FILE_OUTPUT = "data_loker_glints.csv"
JEDA_ANTAR_HALAMAN = 3  # Detik

# Tidak ada filter negara → scraping loker dari seluruh dunia
# ============================================================

print("=" * 60)
print("  GLINTS SCRAPER - TECH & IT EDITION")
print("=" * 60)

options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--window-size=1920,1080')
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                     'Chrome/120.0.0.0 Safari/537.36')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

seen = set()
semua_data_loker = []

def ekstrak_data_dari_halaman(soup, keyword):
    loker_baru = 0
    h2_tags = soup.find_all('h2')

    for h2 in h2_tags:
        parent_div = h2.parent.parent
        if not parent_div:
            continue

        judul = h2.text.strip()
        if not judul:
            continue

        perusahaan = "Dirahasiakan"
        company_container = parent_div.find(attrs={"data-cy": "company_name_job_card"})
        if company_container:
            company_link = company_container.find('a')
            perusahaan = (company_link or company_container).text.strip()

        # Opsi B: Deduplication melibatkan keyword agar loker yang sama
        # dari keyword berbeda tetap diambil (tidak diskip)
        kunci = f"{judul.lower()}|{perusahaan.lower()}|{keyword.lower()}"
        if kunci in seen:
            continue
        seen.add(kunci)

        lokasi = "Tidak diketahui"
        location_elem = parent_div.find(class_=lambda x: x and 'CardJobLocation' in x)
        if location_elem:
            lokasi = location_elem.text.strip()

        tags = parent_div.find_all(class_=lambda x: x and 'TagContentWrapper' in x)
        tag_list = [t.text.strip() for t in tags if t.text.strip() and '+' not in t.text.strip()]

        tipe_pekerjaan  = tag_list[0] if len(tag_list) > 0 else "Tidak diketahui"
        pengalaman      = tag_list[1] if len(tag_list) > 1 else "Tidak diketahui"
        pendidikan      = tag_list[2] if len(tag_list) > 2 else "Tidak diketahui"

        gaji = "Tidak dicantumkan"
        salary_elem = parent_div.find(class_=lambda x: x and 'SalaryWrapper' in x)
        if salary_elem:
            gaji = salary_elem.text.strip()

        tanggal_posting = "Tidak diketahui"
        card_div = parent_div.parent
        if card_div:
            date_elem = card_div.find(class_=lambda x: x and 'UpdatedAtMessage' in x)
            if date_elem:
                tanggal_posting = date_elem.text.strip()

        semua_data_loker.append([
            judul, perusahaan, lokasi,
            tipe_pekerjaan, pengalaman, pendidikan,
            gaji, tanggal_posting, keyword
        ])
        loker_baru += 1

    return loker_baru

try:
    for idx, keyword in enumerate(KEYWORDS):
        print(f"\n[{idx+1}/{len(KEYWORDS)}] Mencari keyword: '{keyword}'")
        halaman_sekarang = 1
        total_keyword = 0
        halaman_kosong_berturut = 0

        while True:
            # Tanpa filter countryCode → hasil dari seluruh dunia
            url = (
                f"https://glints.com/id/opportunities/jobs/explore"
                f"?keyword={keyword.replace(' ', '%20')}"
                f"&page={halaman_sekarang}"
            )
            print(f"  Hal {halaman_sekarang}... ", end="", flush=True)
            
            try:
                driver.get(url)
                time.sleep(JEDA_ANTAR_HALAMAN)

                # Opsi A: Scroll ke bawah untuk memuat konten lazy-load / infinite scroll
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)

                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                ditemukan = ekstrak_data_dari_halaman(soup, keyword)
                print(f"{ditemukan} baru.", end=" ", flush=True)

                total_keyword += ditemukan
                
                if ditemukan == 0:
                    halaman_kosong_berturut += 1
                else:
                    halaman_kosong_berturut = 0

                # Jika 3 halaman berturut-turut kosong, hentikan untuk keyword ini
                if halaman_kosong_berturut >= 3:
                    break
                
                halaman_sekarang += 1
            except Exception as e:
                print(f"Error: {e}")
                break

        print(f"Total: {total_keyword}")

except KeyboardInterrupt:
    print("\nDihentikan pengguna.")
finally:
    driver.quit()

if semua_data_loker:
    with open(NAMA_FILE_OUTPUT, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Judul Posisi', 'Nama Perusahaan', 'Lokasi', 'Tipe Pekerjaan', 'Pengalaman', 'Pendidikan', 'Gaji', 'Tanggal Posting', 'Keyword'])
        writer.writerows(semua_data_loker)
    print(f"\nSELESAI! {len(semua_data_loker)} data tersimpan di {NAMA_FILE_OUTPUT}")
else:
    print("\nTidak ada data didapat.")