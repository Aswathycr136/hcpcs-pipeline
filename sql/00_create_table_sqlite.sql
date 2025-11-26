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