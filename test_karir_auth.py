import requests

LIST_URL = "https://gateway2-beta.karir.com/v2/search/opportunities"
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
payload_list = {"is_opportunity": True, "limit": 20, "offset": 0, "sort_order": "newest"}

print("Testing without Auth...")
res = requests.post(LIST_URL, json=payload_list, headers=headers)
print(res.status_code)
if res.status_code == 200:
    print("Success without auth!")
    print(len(res.json().get("data", {}).get("opportunities", [])))
else:
    print(res.text)

# Try fetching homepage to see if we can get a guest token
# res2 = requests.get("https://karir.com/")
# print("Cookie:", res2.cookies)
