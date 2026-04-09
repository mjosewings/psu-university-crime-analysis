"""
run_cleanup.py
Executes the campus code cleanup on both the SQLite database and JSON file,
then exports the cleaned data to CSV files.

Usage:
    python run_cleanup.py
"""

import sqlite3
import json
import csv
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "psu_crime_log.db")
JSON_PATH = os.path.join(BASE_DIR, "psu_crime_log_records.json")
SQL_PATH = os.path.join(BASE_DIR, "clean_campus_codes.sql")
DATA_DIR = os.path.join(BASE_DIR, "data")

# Mapping of incorrect campus codes to correct ones
CAMPUS_CODE_FIXES = {
    "HN": "HS",    # Hershey
    "ER": "BE",    # Erie (Behrend)
    "BKT": "BK",   # Beaver
    "SL": "SK",    # Schuylkill
    "DS": "DB",    # DuBois
    "FE": "FA",    # Fayette
    "PSHI": "UP",  # University Park
    "ABT": "AB",   # Abington
}

CORRECT_CAMPUS_NAMES = {
    "UP": "University Park",
    "AB": "Abington",
    "AL": "Altoona",
    "BK": "Beaver",
    "BE": "Erie (Behrend)",
    "BR": "Berks",
    "BW": "Brandywine",
    "DL": "Dickinson Law",
    "DB": "DuBois",
    "FA": "Fayette",
    "GA": "Greater Allegheny",
    "GV": "Great Valley",
    "HB": "Harrisburg",
    "HZ": "Hazleton",
    "HS": "Hershey",
    "LV": "Lehigh Valley",
    "MA": "Mont Alto",
    "NK": "New Kensington",
    "SK": "Schuylkill",
    "SH": "Shenango",
    "WB": "Wilkes-Barre",
    "WS": "Worthington Scranton",
    "YK": "York",
}


def clean_database():
    """Execute the SQL cleanup script against the SQLite database."""
    print("=" * 60)
    print("STEP 1: Cleaning SQLite database")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Show before state
    count_before = cursor.execute("SELECT COUNT(*) FROM campuses").fetchone()[0]
    incident_count = cursor.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    print(f"  Before: {count_before} campuses, {incident_count} incidents")

    # Read and execute the SQL script
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    # Execute each statement (strip comment lines, run only UPDATE/DELETE)
    for statement in sql.split(";"):
        # Remove comment-only lines from each chunk
        lines = [l for l in statement.splitlines() if not l.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if clean:
            first_word = clean.split()[0].upper() if clean.split() else ""
            if first_word in ("UPDATE", "DELETE"):
                cursor.execute(clean)
                print(f"  Executed: {clean[:70]}...")

    conn.commit()

    # Show after state
    count_after = cursor.execute("SELECT COUNT(*) FROM campuses").fetchone()[0]
    incident_count_after = cursor.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    orphans = cursor.execute("""
        SELECT COUNT(*) FROM incidents i
        LEFT JOIN campuses c ON i.campus_id = c.campus_id
        WHERE c.campus_id IS NULL
    """).fetchone()[0]

    print(f"\n  After: {count_after} campuses, {incident_count_after} incidents")
    print(f"  Orphaned incidents: {orphans}")

    # Print final campus list
    print("\n  Final campus list:")
    for row in cursor.execute("SELECT campus_code, campus_name FROM campuses ORDER BY campus_code"):
        print(f"    {row[0]} - {row[1]}")

    conn.close()
    print()


def clean_json():
    """Fix campus codes in the JSON records file."""
    print("=" * 60)
    print("STEP 2: Cleaning JSON file")
    print("=" * 60)

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        records = json.load(f)

    fixes_applied = 0
    for record in records:
        old_code = record.get("campus_code", "")
        if old_code in CAMPUS_CODE_FIXES:
            new_code = CAMPUS_CODE_FIXES[old_code]
            record["campus_code"] = new_code
            record["campus"] = CORRECT_CAMPUS_NAMES[new_code]
            fixes_applied += 1

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Verify
    codes_after = sorted(set(r["campus_code"] for r in records))
    print(f"  Records processed: {len(records)}")
    print(f"  Campus code fixes applied: {fixes_applied}")
    print(f"  Unique codes after cleanup: {codes_after}")
    print()


def export_csvs():
    """Export all database tables to CSV files in the data/ directory."""
    print("=" * 60)
    print("STEP 3: Exporting CSV files")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = ["campuses", "incidents", "offense_types", "incident_offenses"]

    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        csv_path = os.path.join(DATA_DIR, f"{table}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        print(f"  Exported {table}: {len(rows)} rows -> data/{table}.csv")

    conn.close()
    print()


def copy_clean_db():
    """Copy the cleaned database to the data/ directory."""
    print("=" * 60)
    print("STEP 4: Copying cleaned .db to data/")
    print("=" * 60)

    import shutil
    os.makedirs(DATA_DIR, exist_ok=True)
    dest = os.path.join(DATA_DIR, "psu_crime_log.db")
    shutil.copy2(DB_PATH, dest)
    print(f"  Copied to data/psu_crime_log.db")
    print()


if __name__ == "__main__":
    print("\nPSU Campus Crime Data - Cleanup Script")
    print("=" * 60)
    clean_database()
    clean_json()
    export_csvs()
    copy_clean_db()
    print("Done! All data cleaned and exported.")
