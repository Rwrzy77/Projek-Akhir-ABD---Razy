# scrap_indeed.py
"""Scraper for Indeed IT job listings (Indonesia) – menggunakan Selenium + undetected-chromedriver.

Kenapa undetected-chromedriver?
  - Indeed menerapkan proteksi Cloudflare/bot-detection sehingga request biasa (requests)
    dikembalikan dengan HTTP 403 "Security Check".
  - undetected-chromedriver menjalankan Chrome asli yang dimodifikasi agar tidak
    terdeteksi sebagai bot.

Kebutuhan:
  - Google Chrome harus sudah terinstall di komputer.
  - pip install undetected-chromedriver selenium tqdm beautifulsoup4
"""

import csv
import random
import time
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False
    print("[WARN] undetected-chromedriver tidak ditemukan. Jalankan: pip install undetected-chromedriver")

# ---------------------------------------------------------------------------
# Konfigurasi – ubah sesuai kebutuhan
# ---------------------------------------------------------------------------
KEYWORDS = [
    "Software Engineer", "Backend Developer", "Frontend Developer", "Fullstack Developer",
    "Web Developer", "Mobile Developer", "Android Developer", "iOS Developer",
    "React Developer", "Vue Developer", "Angular Developer", "Node JS Developer",
    "Python Developer", "Java Developer", "Golang Developer", "Data Scientist",
    "Data Analyst", "Data Engineer", "Machine Learning Engineer", "AI Engineer",
    "Deep Learning Engineer", "Business Intelligence Developer", "Database Administrator",
    "Big Data Specialist", "DevOps Engineer", "Cloud Engineer", "Site Reliability Engineer",
    "System Administrator", "Network Engineer", "Network Administrator", "Infrastructure Engineer",
    "Cyber Security Analyst", "Information Security Specialist", "Penetration Tester",
    "Security Engineer", "Product Manager", "Project Manager IT", "Product Owner",
    "Scrum Master", "Business Analyst IT", "Systems Analyst", "IT Consultant",
    "Solution Architect", "ERP Consultant", "SAP Consultant", "QA Engineer",
    "Software Tester", "Automation Test Engineer", "Quality Assurance Analyst",
    "Manual Tester", "UI UX Designer", "UX Researcher", "Product Designer",
    "Graphic Designer", "Web Designer", "IT Support", "Technical Support Specialist",
    "Game Developer", "Blockchain Developer", "Technical SEO Specialist",
    "Digital Marketing Analyst", "Growth Marketer", "Web Analyst",
    "E-commerce Specialist", "E-commerce Manager", "CRM Specialist",
    "Salesforce Administrator", "HubSpot Specialist", "Product Marketing Manager",
    "Quantitative Analyst", "Financial Risk Analyst", "Supply Chain Analyst",
    "Operations Analyst", "Business Process Analyst", "Business Process Automation Specialist",
    "RPA Specialist", "Fintech Specialist", "ERP Specialist", "Technical Recruiter",
    "IT Recruiter", "Technical Sales Consultant", "Pre-Sales Consultant",
    "Solutions Consultant", "Customer Success Manager", "Technical Account Manager",
    "Technical Writer", "Documentation Specialist", "Instructional Designer",
    "Data Privacy Officer", "IT Compliance Specialist", "IT Auditor", "GIS Analyst",
    "GIS Specialist", "Digital Forensic Examiner", "Bioinformatics Analyst",
    "Computational Biologist", "Digital Specialist", "IT Procurement Specialist"
]

LOCATION = "Worldwide"

PAGES_PER_KW = 1
MAX_SCROLLS = 30
OUTPUT_CSV = "data_indeed_it_jobs.csv"
HEADLESS   = False       # Set True untuk mode headless (tanpa tampilan browser)
                         # Disarankan False agar lebih sulit terdeteksi

FIELDNAMES = [
    "job_title", "company_name", "location", "country",
    "job_type", "experience", "education",
    "salary_min", "salary_max", "salary_raw",
    "skills", "keyword", "post_date", "job_url",
    "platform", "remote_flag",
]

# ---------------------------------------------------------------------------
# Setup driver
# ---------------------------------------------------------------------------
def get_driver():
    """Buat Chrome driver dengan undetected-chromedriver."""
    if not HAS_UC:
        raise RuntimeError("undetected-chromedriver belum terinstall!")

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=en-US")
    # Random User-Agent to reduce detection
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(ua_list)}")
    # Randomize viewport sedikit agar terasa lebih natural
    w = random.randint(1200, 1400)
    h = random.randint(800, 950)
    options.add_argument(f"--window-size={w},{h}")
    if HEADLESS:
        options.add_argument("--headless=new")

    driver = uc.Chrome(options=options, use_subprocess=True)
    return driver

# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------
def build_url(keyword: str, page: int) -> str:
    start = page * 10
    q = keyword.replace(" ", "+")
    loc = LOCATION.replace(" ", "+")
    # id.indeed.com = versi Indonesia (redirect resmi)
    return f"https://id.indeed.com/jobs?q={q}&l={loc}&start={start}&lang=en"

# ---------------------------------------------------------------------------
# Parsing satu halaman
# ---------------------------------------------------------------------------
def parse_page(html: str, keyword: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    #  Selector terbaru berdasarkan inspeksi langsung HTML Indeed Indonesia (Jun 2025)
    # Card utama: div.job_seen_beacon membungkus setiap lowongan
    cards = soup.select("div.job_seen_beacon")

    if not cards:
        # Fallback ke selector lama jika tampilan berubah
        cards = (
            soup.select("li.css-5lfssm")
            or soup.select("a.tapItem")
            or soup.select("div.jobsearch-SerpJobCard")
            or soup.select("div[data-testid='slider_item']")
        )

    for card in cards:
        # ----- Judul -----
        # <h3 class="jobTitle"><a class="jcs-JobTitle"><span title="...">JUDUL</span></a></h3>
        title = None
        title_el = card.select_one("h3.jobTitle a span[title]")
        if not title_el:
            title_el = card.select_one("h3.jobTitle span[title]")
        if not title_el:
            title_el = card.select_one("h3.jobTitle a span")
        if not title_el:
            # fallback ke h2 (desain lama)
            title_el = card.select_one("h2.jobTitle span[title], h2.jobTitle span")
        if title_el:
            title = title_el.get("title") or title_el.get_text(strip=True)

        # ----- Perusahaan -----
        # <span data-testid="company-name">Shopee</span>
        company = None
        company_el = card.select_one("span[data-testid='company-name']")
        if company_el:
            company = company_el.get_text(strip=True)

        # ----- Lokasi -----
        # <div data-testid="text-location">Bangkok</div>
        location = None
        loc_el = card.select_one("div[data-testid='text-location']")
        if loc_el:
            location = loc_el.get_text(strip=True)

        # ----- Gaji -----
        # Tidak selalu muncul; coba beberapa selector
        salary_raw = None
        for sel in [
            "div.salary-snippet-container",
            "[data-testid='attribute_snippet_testid']",
            "div.metadata.salary-snippet-container",
            "div[data-testid='salary-snippet']",
        ]:
            el = card.select_one(sel)
            if el:
                salary_raw = el.get_text(strip=True)
                break

        # ----- Tanggal -----
        post_date = None
        for sel in [
            "span[data-testid='myJobsStateDate']",
            "span.date",
            "[data-testid='jobsearch-JobMetadataHeader-item']",
            ".css-qvloho",
        ]:
            el = card.select_one(sel)
            if el:
                post_date = el.get_text(strip=True)
                break

        # ----- Job URL -----
        # <a class="jcs-JobTitle" data-jk="63bc4c205f722172" href="/rc/clk?...">...
        job_url = None
        link_el = card.select_one("a.jcs-JobTitle[href]")
        if not link_el:
            link_el = card.select_one("a[data-jk][href]")
        if not link_el:
            link_el = card.select_one("h3.jobTitle a[href]")
        if link_el and link_el.has_attr("href"):
            href = link_el["href"]
            if href.startswith("/"):
                job_url = "https://id.indeed.com" + href
            elif href.startswith("http"):
                job_url = href

        # ----- Snippet / deskripsi singkat -----
        snippet = None
        for sel in [
            "div.job-snippet",
            "[data-testid='job-snippet']",
            ".css-9446fg",
        ]:
            el = card.select_one(sel)
            if el:
                snippet = el.get_text(separator=" ", strip=True)
                break

        # ----- Remote flag -----
        remote_texts = ["remote", "wfh", "work from home", "kerja dari rumah"]
        all_text = ((location or "") + " " + (snippet or "")).lower()
        remote_flag = "Remote" if any(rt in all_text for rt in remote_texts) else ""

        if title:  # simpan hanya jika ada judul
            jobs.append({
                "job_title":    title,
                "company_name": company,
                "location":     location,
                "country":      "Indonesia",
                "job_type":     "Full-time",
                "experience":   None,
                "education":    None,
                "salary_min":   None,
                "salary_max":   None,
                "salary_raw":   salary_raw,
                "skills":       snippet,
                "keyword":      keyword,
                "post_date":    post_date,
                "job_url":      job_url,
                "platform":     "Indeed",
                "remote_flag":  remote_flag,
            })

    return jobs

# ---------------------------------------------------------------------------
# Main scraping
# ---------------------------------------------------------------------------
def scrape_indeed():
    if not HAS_UC:
        print("ERROR: undetected-chromedriver tidak terinstall.")
        return

    print("=" * 60)
    print("  Indeed IT Jobs Scraper - Selenium Edition (Anti-Block + Resume)")
    print("=" * 60)
    print(f"Keywords  : {len(KEYWORDS)} keywords")
    print(f"Location  : {LOCATION}")
    print(f"Pages/KW  : {PAGES_PER_KW}  (~{PAGES_PER_KW*10} jobs per keyword)")
    print(f"Output    : {OUTPUT_CSV}")
    print(f"Headless  : {HEADLESS}")
    print()

    # Load existing keywords to resume
    scraped_keywords = set()
    out_path = Path(OUTPUT_CSV)
    file_exists = out_path.exists()

    if file_exists:
        try:
            with open(out_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                if reader.fieldnames:
                    for row in reader:
                        kw = row.get("keyword")
                        if kw:
                            scraped_keywords.add(kw)
            print(f"[RESUME] Ditemukan {len(scraped_keywords)} keyword yang sudah di-scrape sebelumnya.")
        except Exception as e:
            print(f"[WARN] Gagal membaca file CSV lama ({e}). Memulai dari awal.")

    # Initialize CSV header if file doesn't exist
    if not file_exists:
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
            writer.writeheader()

    all_jobs_count = 0
    driver = get_driver()

    try:
        for kw_index, kw in enumerate(tqdm(KEYWORDS, desc="Keywords", unit="kw")):
            if kw in scraped_keywords:
                # print(f"\n  -> Skipping: {kw} (Sudah di-scrape)")
                continue

            kw_jobs_scraped = 0
            for page in tqdm(range(PAGES_PER_KW), desc=f"  {kw}", leave=False, unit="pg"):
                url = build_url(kw, page)
                print(f"\n  -> Fetching ({kw} hal {page}): {url}")
                driver.get(url)

                # Tunggu halaman dimuat
                time.sleep(random.uniform(2, 4))

                # Scroll perlahan ke bawah (agar konten lazy-load termuat)
                for _ in range(MAX_SCROLLS):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.2)

                html = driver.page_source
                jobs = parse_page(html, kw)

                # Cek jika terblokir captcha
                if len(jobs) == 0:
                    if "cloudflare" in html.lower() or "security check" in html.lower() or "challenge" in html.lower() or "captcha" in html.lower() or page == 0:
                        print("\n[WARN] Terdeteksi kemungkinan Captcha/Blokir (0 lowongan ditemukan).")
                        print("     Silakan selesaikan verifikasi Cloudflare/Captcha di browser Chrome secara manual.")
                        print("     Skipping verification pause (automatic resume).")
                        time.sleep(5)
                        # Reload halaman setelah user verifikasi
                        driver.get(url)
                        time.sleep(3)
                        for _ in range(MAX_SCROLLS):
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(0.2)
                        html = driver.page_source
                        jobs = parse_page(html, kw)

                if len(jobs) == 0:
                    print("     [INFO] Tidak ada lowongan lagi untuk keyword ini pada halaman ini.")
                    break

                # Tulis data baru ke CSV (Append Mode) secara real-time
                with open(out_path, "a", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
                    writer.writerows(jobs)

                kw_jobs_scraped += len(jobs)
                all_jobs_count += len(jobs)
                print(f"     Berhasil menambah {len(jobs)} lowongan. Total sesi ini: {all_jobs_count}")

                # Jeda antar halaman
                time.sleep(random.uniform(3, 5))

            # Jeda antar keyword
            time.sleep(random.uniform(2, 4))
            if kw_index % 10 == 0 and kw_index != 0:
                print("\n[INFO] Mengambil jeda istirahat sejenak (15 detik) untuk mengurangi rate limit...")
                time.sleep(15)

    except KeyboardInterrupt:
        print("\n[INFO] Scraping dihentikan oleh pengguna.")
    finally:
        driver.quit()

    print(f"\n[OK] Proses selesai! Berhasil menambahkan {all_jobs_count} lowongan baru ke '{OUTPUT_CSV}'.")
    return all_jobs_count

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    scrape_indeed()
