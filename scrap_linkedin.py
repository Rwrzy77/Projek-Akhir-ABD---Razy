from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from kafka_helper import get_kafka_producer, publish_scraped_job
import time
import pandas as pd

# IT & Hybrid keywords list
# Mix job titles and tech to get more variations
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
LOCATION = "Worldwide"

# Setup Selenium with bot detection bypass
print("MEMULAI EKSPEDISI PUKAT HARIMAU LINKEDIN (GLOBAL & ANTI-BLOKIR)...")
print(f"Total target: {len(KEYWORDS)} kata kunci.")
print("[WARN] Peringatan: Proses ini akan memakan waktu cukup lama.")
print("Pastikan laptop menyala dan TIDAK masuk mode SLEEP.")
print("-" * 60)

producer = get_kafka_producer()
if producer:
    print("Kafka Producer terhubung. Data hasil scraping akan dikirim langsung ke Kafka!")
else:
    print("[WARN] Kafka Producer tidak terhubung. Data hanya akan disimpan ke CSV.")

options = webdriver.ChromeOptions()
options.add_argument("--disable-notifications")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# Set custom user-agent to avoid detection
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Hide webdriver property
driver.execute_script("""
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined})
""")

all_jobs = []

# Search and scrape loop
wait = WebDriverWait(driver, 15)

for idx, keyword in enumerate(KEYWORDS, 1):
    print(f"\n[{idx}/{len(KEYWORDS)}] Memancing data untuk: {keyword.upper()}...")

    # Load search page for keyword
    URL = f"https://www.linkedin.com/jobs/search?keywords={keyword.replace(' ', '%20')}&location={LOCATION}"

    try:
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.jobs-search__results-list")))
    except Exception as e:
        print(f"   [GAGAL] Tidak bisa load halaman untuk {keyword}: {e}")
        continue

    time.sleep(2)

    last_card_count = 0
    no_change_count = 0
    max_scrolls = 60

    # Scroll to load job cards
    for i in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # Click "See more jobs" button if visible
        try:
            see_more_button = driver.find_element(By.CSS_SELECTOR, "button.infinite-scroller__show-more-button")
            driver.execute_script("arguments[0].click();", see_more_button)
            time.sleep(1.5)
        except:
            pass

        # Get count of job cards
        job_cards = driver.find_elements(By.CSS_SELECTOR, "ul.jobs-search__results-list > li")
        current_card_count = len(job_cards)

        # Stop if no jobs found after scroll
        if current_card_count == 0 and i >= 2:
            print("   [INFO] Tidak ada lowongan ditemukan untuk kata kunci ini.")
            break

        if current_card_count == last_card_count:
            no_change_count += 1
        else:
            no_change_count = 0

        last_card_count = current_card_count

        # Stop scrolling if reached optimal limit (150)
        if current_card_count >= 150:
            print(f"   [INFO] Sudah memuat {current_card_count} lowongan (mencapai batas optimal). Menghentikan scroll.")
            break

        # Check for no updates after 15 scrolls
        if i >= 15 and no_change_count >= 8:
            print(f"   [INFO] Scroll ke-{i+1}: Tidak ada penambahan lowongan baru. Menghentikan scroll.")
            break

    # Extract data from HTML
    job_cards = driver.find_elements(By.CSS_SELECTOR, "ul.jobs-search__results-list > li")
    found_count = 0

    for card in job_cards:
        try:
            title_elem = card.find_elements(By.CSS_SELECTOR, "h3.base-search-card__title")
            company_elem = card.find_elements(By.CSS_SELECTOR, "h4.base-search-card__subtitle")
            location_elem = card.find_elements(By.CSS_SELECTOR, "span.job-search-card__location")
            link_elem = card.find_elements(By.CSS_SELECTOR, "a.base-card__full-link")
            time_elem = card.find_elements(By.CSS_SELECTOR, "time")

            # Ensure title and company elements exist
            if title_elem and company_elem:
                # Extract text
                title = title_elem[0].get_attribute("innerText").strip()
                company = company_elem[0].get_attribute("innerText").strip()
                location = location_elem[0].get_attribute("innerText").strip() if location_elem else "Tidak dicantumkan"
                link = link_elem[0].get_attribute("href") if link_elem else "Tidak dicantumkan"
                
                posted_date = ""
                if time_elem:
                    posted_date = time_elem[0].get_attribute("datetime")
                    if not posted_date:
                        posted_date = time_elem[0].get_attribute("innerText").strip()
                if not posted_date:
                    posted_date = "Tidak dicantumkan"

                # Store job if title is not empty
                if title:
                    job_data = {
                        "searched_keyword": keyword,
                        "job_title": title,
                        "company_name": company,
                        "location": location,
                        "job_link": link,
                        "posted_date": posted_date,
                        "source_platform": "LinkedIn"
                    }
                    all_jobs.append(job_data)
                    if producer:
                        publish_scraped_job(producer, job_data, "LinkedIn")
                    found_count += 1
        except Exception as e:
            continue

    print(f"   [INFO] Berhasil menjaring {found_count} data kotor dari keyword ini.")
    time.sleep(2)

# Close browser
driver.quit()
if producer:
    producer.flush()
    producer.close()

# Clean and save to CSV
if all_jobs:
    df = pd.DataFrame(all_jobs)
    
    # Remove duplicates
    df_clean = df.drop_duplicates(subset=['job_title', 'company_name'])
    
    # Save output to CSV file
    file_name = "linkedin_jobs_Ribuan.csv"
    df_clean.to_csv(file_name, index=False, encoding="utf-8-sig", sep=";")
    
    print("\n" + "=" * 60)
    print(f"EKSPEDISI SELESAI!")
    print(f"Total Data Terkumpul (Kotor): {len(all_jobs)} baris")
    print(f"Total Data Unik (Bersih): {len(df_clean)} baris")
    print(f"File tersimpan sebagai: {file_name}")
    print("=" * 60)
else:
    print("\n[GAGAL] Tidak ada data yang berhasil dikumpulkan.")