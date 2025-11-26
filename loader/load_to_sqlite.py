# loader/load_to_sqlite.py
import sqlite3, json, glob, os
from pathlib import Path
from dateutil import parser as dateparser

OUT_DIR = Path("../scraper_output")
DB_FILE = Path("../hcpcs.db")

def parse_date(s):
    if not s:
        return None
    try:
        return dateparser.parse(s).date().isoformat()
    except Exception:
        return None

def ensure_schema(conn):
    with conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS hcpcs_codes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          group_code TEXT NOT NULL,
          category_name TEXT NOT NULL,
          hcpcs_code TEXT NOT NULL,
          long_description TEXT NOT NULL,
          effective_date TEXT DEFAULT (date('now')),
          end_date TEXT,
          created_at TEXT DEFAULT (datetime('now'))
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hcpcs_code ON hcpcs_codes(hcpcs_code);")

def load_file(conn, file_path):
    with open(file_path, "r", encoding="utf8") as f:
        data = json.load(f)
    rows = []
    for r in data:
        rows.append((
            r.get("group_code"),
            r.get("category_name"),
            r.get("hcpcs_code"),
            r.get("long_description"),
            parse_date(r.get("effective_date")),
            parse_date(r.get("end_date"))
        ))
    with conn:
        conn.executemany("""
        INSERT INTO hcpcs_codes (group_code, category_name, hcpcs_code, long_description, effective_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?);
        """, rows)

def main():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(glob.glob(str(OUT_DIR / "hcpcs_*.json")))
    if not files:
        print("No scraper output found in", OUT_DIR)
        return
    conn = sqlite3.connect(str(DB_FILE))
    ensure_schema(conn)
    for f in files:
        print("Loading", f)
        load_file(conn, f)
    conn.close()
    print("Loaded files into", DB_FILE)

if __name__ == "__main__":
    main()
