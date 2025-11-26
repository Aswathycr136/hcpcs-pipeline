CREATE TABLE IF NOT EXISTS hcpcs_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    group_code TEXT,
    category_name TEXT,
    hcpcs_code TEXT,

    short_description TEXT,
    long_description TEXT,
    detailed_description TEXT,

    effective_date TEXT,
    end_date TEXT,

    action_code TEXT,
    pricing_indicator_code TEXT,
    status_code TEXT,

    created_at TEXT DEFAULT (datetime('now'))
);
