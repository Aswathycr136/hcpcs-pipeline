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

def extract_group_code(category_name):
    """Extract the first real uppercase letter from category like 'A' Codes."""
    match = re.search(r"[A-Z]", category_name)
    return match.group(0) if match else None

def extract_code_table(html, category_name):
    """Extract HCPCS code table and visit detail pages."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    group_code = extract_group_code(category_name)

    table = soup.find("table")
    if not table:
        print(f"No table found for category: {category_name}")
        return rows

    for tr in table.select("tbody tr"):
        cols = tr.find_all("td")
        if len(cols) < 1:
            continue

        # Get hcpcs code and detail link
        code_tag = cols[0].find("a")
        if not code_tag:
            continue
        hcpcs_code = code_tag.get_text(strip=True)
        detail_url = urljoin(BASE, code_tag.get("href"))

        # Fetch detail page
        try:
            detail_html = fetch(detail_url)
            detail_soup = BeautifulSoup(detail_html, "html.parser")

            # long_description from <h1>
            h1_tag = detail_soup.find("h1")
            long_description = h1_tag.get_text(strip=True) if h1_tag else ""

            # short_description from first <p> or table
            p_tag = detail_soup.find("p")
            short_description = p_tag.get_text(strip=True) if p_tag else ""

            # Effective date
            eff_date_tag = detail_soup.find(text=re.compile(r"HCPCS Action Effective Date", re.I))
            effective_date = ""
            if eff_date_tag:
                parent = eff_date_tag.find_parent()
                if parent:
                    effective_date = parent.get_text(strip=True).replace("HCPCS Action Effective Date", "").strip()

            # End date (optional)
            end_date = None
            end_date_tag = detail_soup.find(text=re.compile(r"End Date", re.I))
            if end_date_tag:
                parent = end_date_tag.find_parent()
                if parent:
                    end_date = parent.get_text(strip=True).replace("End Date", "").strip()

            # Append row
            rows.append({
                "id": None,  # will fill later
                "group_code": group_code,
                "category_name": category_name,
                "hcpcs_code": hcpcs_code,
                "long_description": long_description,
                "short_description": short_description,
                "effective_date": effective_date,
                "end_date": end_date
            })

            time.sleep(0.5)  # politeness

        except Exception as e:
            print(f"  ERROR fetching detail for {hcpcs_code}: {e}")
            continue

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

    # Assign incremental IDs
    for i, row in enumerate(all_data, start=1):
        row["id"] = i

    output_file = OUTPUT_DIR / "hcpcs_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print("Saved data to:", output_file)
    print("Scraper completed successfully.")

if __name__ == "__main__":
    main()
