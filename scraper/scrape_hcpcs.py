import requests, time, json, sys
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin

BASE = "https://www.hcpcsdata.com"
START = "https://www.hcpcsdata.com/Codes"
OUT = Path("../scraper_output")
OUT.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "hcpcs-scraper/1.0"}

def fetch(url):
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.text

def parse_categories(html):
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select("a[href*='/Codes/']")
    out = []
    for a in links:
        url = urljoin(BASE, a.get("href"))
        name = a.get_text(strip=True)
        out.append({"name": name, "url": url})
    return out

def parse_codes(html, category_name):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table tbody tr"):
        cols = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cols) >= 2:
            rows.append({
                "group_code": category_name[:1],
                "category_name": category_name,
                "hcpcs_code": cols[0],
                "long_description": cols[1],
                "effective_date": cols[2] if len(cols) > 2 else None,
                "end_date": cols[3] if len(cols) > 3 else None
            })
    return rows

def main():
    html = fetch(START)
    cats = parse_categories(html)
    data = []
    for c in cats:
        print("Scraping:", c["name"])
        html = fetch(c["url"])
        data.extend(parse_codes(html, c["name"]))
        time.sleep(1)

    out_file = OUT / "hcpcs_data.json"
    json.dump(data, out_file.open("w"), indent=2)
    print("Saved:", out_file)

if __name__ == "__main__":
    main()
