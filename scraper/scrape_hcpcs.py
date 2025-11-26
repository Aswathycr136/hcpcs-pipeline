import json
import re
import time
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://www.hcpcsdata.com"
START_URL = f"{BASE}/Codes"
OUTPUT_DIR = Path("../scraper_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (HCPCS Optimized Scraper)"
})

def fetch(url):
    """Fetch a URL using persistent session."""
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def extract_group_code(text):
    m = re.search(r"[A-Z]", text)
    return m.group(0) if m else None

def extract_categories(html):
    soup = BeautifulSoup(html, "html.parser")
    categories = []
    for a in soup.select("a[href*='/Codes/']"):
        href = a.get("href")
        if href and "/Codes/" in href:
            categories.append({
                "name": a.get_text(strip=True),
                "url": urljoin(BASE, href)
            })
    return categories

def extract_codes_from_category(html, category_name):
    soup = BeautifulSoup(html, "html.parser")
    group_code = extract_group_code(category_name)

    codes = []
    table = soup.find("table")
    if not table:
        return codes

    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        code_tag = tds[0].find("a")
        if not code_tag:
            continue

        codes.append({
            "group_code": group_code,
            "category_name": category_name,
            "hcpcs_code": code_tag.get_text(strip=True),
            "short_description": tds[1].get_text(strip=True),
            "detail_url": urljoin(BASE, code_tag.get("href"))
        })

    return codes

def extract_detail(item):
    """Scrape detail page for a single code."""
    try:
        html = fetch(item["detail_url"])
        soup = BeautifulSoup(html, "html.parser")

        h1 = soup.find("h1")
        p = soup.find("p")

        item["long_description"] = h1.get_text(strip=True) if h1 else None
        item["detailed_description"] = p.get_text(" ", strip=True) if p else None

        info = soup.find("table")
        if info:
            for tr in info.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) == 2:
                    key = tds[0].get_text(strip=True).lower().replace(" ", "_")
                    value = tds[1].get_text(" ", strip=True)
                    item[key] = value

    except Exception as e:
        item["error"] = str(e)

    return item

def main():
    print("Fetching categories...")
    home_html = fetch(START_URL)
    categories = extract_categories(home_html)

    print(f"Found {len(categories)} categories")

    all_codes = []

    for cat in categories:
        print(f"\nScraping category: {cat['name']}")
        html = fetch(cat["url"])
        codes = extract_codes_from_category(html, cat["name"])
        all_codes.extend(codes)
        print(f"  → {len(codes)} codes found")
        time.sleep(0.05)

    print(f"\nTotal codes: {len(all_codes)}")
    print("Scraping detail pages in parallel...")

    results = []
    with ThreadPoolExecutor(max_workers=30) as pool:
        tasks = [pool.submit(extract_detail, item) for item in all_codes]
        for i, task in enumerate(as_completed(tasks), 1):
            results.append(task.result())
            if i % 200 == 0:
                print(f"  → {i}/{len(all_codes)} done")

    outfile = OUTPUT_DIR / "hcpcs_data_full.json"
    json.dump(results, open(outfile, "w", encoding="utf-8"), indent=2)

    print("\nSaved:", outfile)
    print("Scraping completed.")

if __name__ == "__main__":
    main()
