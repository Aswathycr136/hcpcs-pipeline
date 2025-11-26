# scrape_hcpcs.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin

BASE_URL = "https://www.hcpcsdata.com"
START_URL = urljoin(BASE_URL, "/Codes")

# Use a realistic User-Agent to avoid 403
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36"
}

def fetch(url):
    """Fetch page content with headers."""
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
    """Parse category links from the main page."""
    soup = BeautifulSoup(html, "html.parser")
    category_links = []

    for a in soup.select("div.codes a"):  # check actual selector on the site
        href = a.get("href")
        if href:
            category_links.append(urljoin(BASE_URL, href))
    return category_links

def parse_code_page(html):
    """Parse code data from a category page."""
    soup = BeautifulSoup(html, "html.parser")
    data = []

    rows = soup.select("table tbody tr")  # check selector
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
        time.sleep(1)  # delay to avoid blocking

    # Save to CSV
    df = pd.DataFrame(all_data)
    df.to_csv("hcpcs_codes.csv", index=False)
    print(f"Saved {len(df)} codes to hcpcs_codes.csv")

if __name__ == "__main__":
    main()
