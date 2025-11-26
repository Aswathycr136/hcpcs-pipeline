import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

BASE = "https://www.hcpcsdata.com"
START = f"{BASE}/Codes"

# Strong browser headers to bypass block
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive",
}

# Request function with retry + random pause
def fetch(url, retries=5):
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.5, 3.0))  # slow down to avoid block
            r = requests.get(url, headers=HEADERS, timeout=20)

            if r.status_code == 403:
                print("403 detected → retrying with delay…")
                time.sleep(random.uniform(4, 7))
                continue

            r.raise_for_status()
            return r.text

        except Exception as e:
            print(f"Error fetching {url}. Attempt {attempt+1}/{retries}")
            time.sleep(random.uniform(2, 5))

    raise Exception(f"Failed after {retries} attempts → {url}")


def extract_table(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")

    if table is None:
        raise ValueError("No table found on page")

    rows = table.find_all("tr")
    data = []
    for row in rows:
        cols = [c.text.strip() if c.text.strip() != "" else None for c in row.find_all("td")]
        if cols:
            data.append(cols)

    return data


def main():
    print("Fetching main codes page…")
    html = fetch(START)

    print("Extracting tables…")
    data = extract_table(html)

    df = pd.DataFrame(data)
    df.columns = ["Code", "Description", "Pricing Info", "Actions"]

    df.to_csv("hcpcs_data.csv", index=False)
    print("Saved → hcpcs_data.csv")


if __name__ == "__main__":
    main()
