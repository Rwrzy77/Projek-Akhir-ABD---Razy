"""Quick test: cek apakah Indeed bisa diakses dan apakah card selector benar."""
import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA}
URL = "https://www.indeed.com/jobs?q=software+engineer&l=Indonesia&start=0"

print("Fetching:", URL)
try:
    r = requests.get(URL, headers=HEADERS, timeout=15)
    print("HTTP Status:", r.status_code)
    soup = BeautifulSoup(r.text, "html.parser")
    print("Page Title:", soup.title.text.strip() if soup.title else "No title")

    # Try common card selectors
    tap  = soup.select("a.tapItem")
    serp = soup.select("div.jobsearch-SerpJobCard")
    li   = soup.select("li.css-5lfssm")        # newer Indeed
    job  = soup.select("[data-testid='job-title']")  # another common pattern

    print(f"a.tapItem              : {len(tap)}")
    print(f"div.jobsearch-SerpJobCard: {len(serp)}")
    print(f"li.css-5lfssm          : {len(li)}")
    print(f"[data-testid=job-title]: {len(job)}")

    if len(tap) == 0 and len(serp) == 0:
        print("\nINDEED MEMBLOKIR PERMINTAAN (bot detection).")
        print("Gunakan Selenium / diperlukan tindakan tambahan.")
        # print 100 char dari body untuk cek
        print("\n--- Preview HTML (500 chars) ---")
        print(r.text[:500])
    else:
        print("\nBerhasil menemukan job cards!")
        # print salah satu card
        first = (tap or serp)[0]
        print("\n--- Contoh Job Card (text) ---")
        print(first.get_text(separator=" | ", strip=True)[:300])
except Exception as e:
    print("ERROR:", e)
