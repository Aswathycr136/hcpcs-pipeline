import sqlite3

DB = "../hcpcs.db"

def q(conn, sql):
    return conn.execute(sql).fetchall()

def print_section(title, rows):
    print("\n" + title)
    if not rows:
        print("(no results)")
    else:
        for r in rows:
            print(r)

def main():
    conn = sqlite3.connect(DB)

    # 1. Write a query to count the number of codes in each group.
    rows = q(conn, "SELECT group_code, COUNT(*) FROM hcpcs_codes GROUP BY group_code")
    print_section("Counts per group:", rows)

    # 2.  Show the top 5 categories with the highest number of codes.
    rows = q(conn, """
        SELECT category_name, COUNT(*)
        FROM hcpcs_codes
        GROUP BY category_name
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    print_section("Top 5 categories:", rows)

    # 3. Find all codes that have changed descriptions
    rows = q(conn, """
        SELECT hcpcs_code, COUNT(DISTINCT long_description)
        FROM hcpcs_codes
        GROUP BY hcpcs_code
        HAVING COUNT(DISTINCT long_description) > 1;
    """)
    print_section("Codes with multiple descriptions:", rows)

    # 4. Shows Codes that were active in 2022 but expired before 2024
    rows = q(conn, """
        SELECT hcpcs_code, effective_date, end_date
        FROM hcpcs_codes
        WHERE effective_date <= '2022-12-31'
          AND end_date IS NOT NULL
          AND end_date >='2022-01-01'
          AND end_date < '2024-01-01';
    """)
    print_section("Active in 2022 but expired before 2024:", rows)

    conn.close()
    print("\nValidation done.")

if __name__ == "__main__":
    main()
