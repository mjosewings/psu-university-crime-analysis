"""
Add Time Features
=================
Parses reported_datetime from the raw incidents table and adds:
  year, month, month_name, hour, day_of_week, is_weekend, time_bucket, date

Usage:
    python add_time_features.py                     # reads data/raw/psu_crime_log.db
    python add_time_features.py --db path/to/db     # custom DB path
    python add_time_features.py --out data/processed/incidents.csv
"""

import argparse
import sqlite3
from pathlib import Path
import pandas as pd


def get_time_bucket(hour: int) -> str:
    if 6 <= hour < 12:  return "Morning"
    if 12 <= hour < 18: return "Afternoon"
    if 18 <= hour < 22: return "Evening"
    return "Late Night"


def build_dataframe(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT i.id, i.incident_number, i.campus_id,
               cam.campus_name AS final_campus,
               i.reported_datetime, i.occurred_start, i.occurred_end,
               i.nature_of_incident, i.location, i.created_at
        FROM incidents i
        JOIN campuses cam ON i.campus_id = cam.campus_id
        """,
        conn,
    )
    conn.close()

    dt = pd.to_datetime(df["reported_datetime"], format="%m/%d/%Y %I:%M %p", errors="coerce")
    df["date"]        = dt.dt.date
    df["year"]        = dt.dt.year
    df["month"]       = dt.dt.month
    df["month_name"]  = dt.dt.strftime("%B")
    df["hour"]        = dt.dt.hour
    df["day_of_week"] = dt.dt.day_name()
    df["is_weekend"]  = df["day_of_week"].isin(["Saturday", "Sunday"])
    df["time_bucket"] = df["hour"].apply(get_time_bucket)

    n_bad = dt.isna().sum()
    if n_bad:
        print(f"[!] {n_bad} rows had unparseable dates — hour/day will be NaN for those rows.")

    return df


def main():
    parser = argparse.ArgumentParser(description="Add time features to PSU incidents")
    parser.add_argument("--db",  default="data/raw/psu_crime_log.db",
                        help="Path to SQLite database")
    parser.add_argument("--out", default="data/processed/incidents.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    print(f"[+] Loading from: {args.db}")
    df = build_dataframe(args.db)
    print(f"[+] Loaded {len(df):,} incidents across {df['final_campus'].nunique()} campuses")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"[+] Saved → {args.out}")


if __name__ == "__main__":
    main()
