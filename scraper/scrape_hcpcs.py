import requests
import time
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

BASE = "https://www.hcpcsdata.com"
START_URL = f"{BASE}/Codes"
OUTPUT_DIR = Path("../scraper_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (HCPCS Pipeline Scraper - github.com/Aswathycr136/hcpcs-pipeline)"
}

def fetch(url, retry=3):
    for attempt in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except:
            time.sleep(2)
    raise RuntimeError(f"Failed to fetch {url}")

def extract_group_code(category_name):
    m = re.search(r"[A-Z]", category_name)
    return m.group(0) if m else None

def extract_categories(html):
    soup = BeautifulSoup(html, "html.parser")
    categories = []

    for a in soup.select("a[href*='/Codes/']"):
        name = a.get_text(strip=True)
        href = a.get("href")
        if href and name:
            categories.append({
                "name": name,
                "url": urljoin(BASE, href)
            })
    return categories

def extract_category_codes(html, category_name):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    group_code = extract_group_code(category_name)

    table = soup.find("table")
    if not table:
        return rows

    for tr in table.select("tbody tr"):
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue

        code_link = cols[0].find("a")
        if not code_link:
            continue

        code = code_link.get_text(strip=True)
        detail_url = urljoin(BASE, code_link.get("href"))
        desc = cols[1].get_text(strip=True)

        rows.append({
            "group_code": group_code,
            "category_name": category_name,
            "hcpcs_code": code,
            "short_description": desc,
            "detail_url": detail_url
        })

    return rows

def extract_detail_page(url):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    result = {}

    # Long description
    h1 = soup.find("h1")
    if h1:
        result["long_description"] = h1.get_text(strip=True)

    # Definition / description block
    p = soup.find("p")
    if p:
        result["detailed_description"] = p.get_text(" ", strip=True)

    # Extract key/value rows from detail page table
    info_table = soup.find("table")
    if info_table:
        for tr in info_table.select("tr"):
            tds = tr.find_all("td")
            if len(tds) == 2:
                key = tds[0].get_text(strip=True).lower().replace(" ", "_")
                value = tds[1].get_text(" ", strip=True)
                result[key] = value

    return result


def main():
    print("Fetching main page…")
    home_html = fetch(START_URL)

    print("Extracting categories…")
    categories = extract_categories(home_html)
    print(f"Found {len(categories)} categories.")

    all_rows = []

    for cat in categories:
        print(f"\nCategory: {cat['name']}")
        cat_html = fetch(cat["url"])

        codes = extract_category_codes(cat_html, cat["name"])
        print(f"  Found {len(codes)} codes")

        for item in codes:
            print(f"    → Fetching detail: {item['hcpcs_code']}")
            detail = extract_detail_page(item["detail_url"])
            item.update(detail)
            all_rows.append(item)
            time.sleep(1)

    output_file = OUTPUT_DIR / "hcpcs_data_full.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2, ensure_ascii=False)

    print("\nSaved:", output_file)
    print("Scraper completed.")

if __name__ == "__main__":
    main()
