import json
import sqlite3
from pathlib import Path
from dateutil import parser

DB = Path("../hcpcs.db")
JSON_FILE = Path("../scraper_output/hcpcs_data_full.json")
SQL_FILE = Path("../sql/00_create_table_sqlite.sql")

def dt(x):
    if not x: 
        return None
    try:
        return parser.parse(x).date().isoformat()
    except:
        return None

def main():
    data = json.load(open(JSON_FILE, "r", encoding="utf-8"))

    conn = sqlite3.connect(DB)
    conn.executescript(open(SQL_FILE).read())

    rows = []
    for r in data:
        rows.append((
            r.get("group_code"),
            r.get("category_name"),
            r.get("hcpcs_code"),
            r.get("short_description"),
            r.get("long_description"),
            r.get("detailed_description"),
            dt(r.get("effective_date")),
            dt(r.get("termination_date")),
            r.get("action_code"),
            r.get("pricing_indicator_code"),
            r.get("status_code")
        ))

    conn.executemany("""
        INSERT INTO hcpcs_codes (
            group_code, category_name, hcpcs_code,
            short_description, long_description, detailed_description,
            effective_date, end_date,
            action_code, pricing_indicator_code, status_code
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()

    print("Loaded", len(rows), "rows into SQLite.")

if __name__ == "__main__":
    main()
