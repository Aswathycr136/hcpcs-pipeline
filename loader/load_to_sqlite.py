import sqlite3, json
from pathlib import Path
from dateutil import parser

DB = Path("../hcpcs.db")
JSON_FILE = Path("../scraper_output/hcpcs_data.json")
SQL_FILE = Path("../sql/00_create_table_sqlite.sql")

def parse_date(d):
    try:
        return parser.parse(d).date().isoformat() if d else None
    except:
        return None

def main():
    conn = sqlite3.connect(DB)
    conn.execute(open(SQL_FILE).read())

    data = json.load(open(JSON_FILE))
    rows = []
    for r in data:
        rows.append((
            r["group_code"],
            r["category_name"],
            r["hcpcs_code"],
            r["long_description"],
            parse_date(r["effective_date"]),
            parse_date(r["end_date"])
        ))

    conn.executemany("""
    INSERT INTO hcpcs_codes
    (group_code, category_name, hcpcs_code, long_description, effective_date, end_date)
    VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    print("Loaded into SQLite:", DB)

if __name__ == "__main__":
    main()
