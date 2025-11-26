import requests
import time
import json
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
    """Fetch webpage with retries."""
    for attempt in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"Fetch error: {e}. Retrying...")
            time.sleep(2)
    raise RuntimeError(f"Failed to fetch {url}")

def extract_categories(html):
    """Extract category links from the main Codes page."""
    soup = BeautifulSoup(html, "html.parser")
    categories = []

    for a in soup.select("a[href*='/Codes/']"):
        name = a.get_text(strip=True)
        href = a.get("href")

        if href and name:
            url = urljoin(BASE, href)
            categories.append({"name": name, "url": url})

    return categories

def extract_code_table(html, category_name):
    """Extract HCPCS code table using HTML parsing (no pandas)."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    table = soup.find("table")
    if not table:
        print(f"No table found for category: {category_name}")
        return rows

    for tr in table.select("tbody tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]

        if len(cols) >= 2:
            code = cols[0]
            desc = cols[1]
            eff_date = cols[2] if len(cols) > 2 else None
            end_date = cols[3] if len(cols) > 3 else None

            rows.append({
                "group_code": category_name[:1],  
                "category_name": category_name,
                "hcpcs_code": code,
                "long_description": desc,
                "effective_date": eff_date,
                "end_date": end_date
            })

    return rows

def main():
    print("Fetching main codes page…")
    home_html = fetch(START_URL)

    print("Extracting categories…")
    categories = extract_categories(home_html)
    print(f"Found {len(categories)} categories.")

    all_data = []

    for cat in categories:
        print(f"Processing category: {cat['name']}")

        try:
            page_html = fetch(cat["url"])
            items = extract_code_table(page_html, cat["name"])
            print(f"  → Extracted {len(items)} rows")
            all_data.extend(items)
        except Exception as e:
            print(f"ERROR processing {cat['name']}: {e}")

        time.sleep(1)  # Politeness

    output_file = OUTPUT_DIR / "hcpcs_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print("Saved data to:", output_file)
    print("Scraper completed successfully.")

if __name__ == "__main__":
    main()
