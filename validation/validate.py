# validation/validate.py
import sqlite3, os, sys

DB_FILE = os.getenv("DB_FILE", "../hcpcs.db")

def run_query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()

def main():
    if not os.path.exists(DB_FILE):
        print("DB file not found:", DB_FILE, file=sys.stderr)
        sys.exit(2)
    conn = sqlite3.connect(DB_FILE)
    print("Counts per group:")
    for row in run_query(conn, "SELECT group_code, COUNT(*) FROM hcpcs_codes GROUP BY group_code;"):
        print(row)
    print("\nTop 5 categories:")
    for row in run_query(conn, "SELECT category_name, COUNT(*) AS cnt FROM hcpcs_codes GROUP BY category_name ORDER BY cnt DESC LIMIT 5;"):
        print(row)
    print("\nCodes with multiple descriptions:")
    for row in run_query(conn, "SELECT hcpcs_code, COUNT(DISTINCT long_description) as versions FROM hcpcs_codes GROUP BY hcpcs_code HAVING versions > 1;"):
        print(row)
    print("\nActive in 2022 but expired before 2024:")
    for row in run_query(conn, "SELECT hcpcs_code, effective_date, end_date FROM hcpcs_codes WHERE effective_date <= '2022-12-31' AND end_date IS NOT NULL AND end_date < '2024-01-01';"):
        print(row)
    conn.close()
    print("\nValidation done")
    sys.exit(0)

if __name__ == "__main__":
    main()
