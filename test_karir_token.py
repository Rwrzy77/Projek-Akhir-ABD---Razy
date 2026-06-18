from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

options = Options()
options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

driver.get("https://karir.com/")
time.sleep(3)
# Try getting token from local storage
token = driver.execute_script("return window.localStorage.getItem('token');")
print("Local Storage Token:", token)

# If none, maybe it's accessible via cookies?
cookies = driver.get_cookies()
print("Cookies:", cookies)

driver.quit()
