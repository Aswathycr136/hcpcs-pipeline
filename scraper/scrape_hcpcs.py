# scrape_hcpcs.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import os

BASE_URL = "https://www.hcpcsdata.com"
START_URL = urljoin(BASE_URL, "/Codes")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36"
}

OUTPUT_FOLDER = "../scraper_output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
CSV_FILE = os.path.join(OUTPUT_FOLDER, "hcpcs_data_full.csv")
JSON_FILE = os.path.join(OUTPUT_FOLDER, "hcpcs_data_full.json")

def fetch(url):
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def parse_categories(html):
    soup = BeautifulSoup(html, "html.parser")
    category_links = []
    for a in soup.select("div.codes a"):  # adjust selector if needed
        href = a.get("href")
        if href:
            category_links.append(urljoin(BASE_URL, href))
    return category_links

def parse_code_page(html):
    soup = BeautifulSoup(html, "html.parser")
    data = []
    rows = soup.select("table tbody tr")  # adjust selector if needed
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            code = cols[0].text.strip()
            desc = cols[1].text.strip()
            data.append({"Code": code, "Description": desc})
    return data

def main():
    print("Fetching categories...")
    home_html = fetch(START_URL)
    if not home_html:
        print("Failed to fetch main page. Exiting.")
        return

    categories = parse_categories(home_html)
    print(f"Found {len(categories)} categories.")

    all_data = []

    for idx, url in enumerate(categories, start=1):
        print(f"[{idx}/{len(categories)}] Fetching {url}")
        html = fetch(url)
        if html:
            codes = parse_code_page(html)
            all_data.extend(codes)
        else:
            print(f"Failed to fetch {url}")
        time.sleep(1)

    df = pd.DataFrame(all_data)
    df.to_csv(CSV_FILE, index=False)
    df.to_json(JSON_FILE, orient="records", force_ascii=False)
    print(f"Saved {len(df)} codes to CSV and JSON in {OUTPUT_FOLDER}")

if __name__ == "__main__":
    main()
