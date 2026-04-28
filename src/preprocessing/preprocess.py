"""
PSU Crime Data Preprocessing
=============================
Loads raw JSON, engineers time features, and exports per-campus CSVs.

Usage:
    python preprocess.py
    python preprocess.py --input data/raw/psu_crime_log_records.json
"""

import json, csv, os, re, argparse
from datetime import datetime
from collections import Counter

RAW_JSON  = os.path.join(os.path.dirname(__file__), "../../data/raw/psu_crime_log_records.json")
PROC_DIR  = os.path.join(os.path.dirname(__file__), "../../data/processed")
CAMP_DIR  = os.path.join(os.path.dirname(__file__), "../../data/campuses")


def parse_dt(s: str):
    if not s: return None
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try: return datetime.strptime(s.strip(), fmt)
        except: pass
    return None


def time_bucket(h):
    if h is None: return "Unknown"
    if  6 <= h < 12: return "Morning"
    if 12 <= h < 18: return "Afternoon"
    if 18 <= h < 22: return "Evening"
    return "Late Night"


def process(input_path: str = None):
    path = input_path or RAW_JSON
    print(f"[+] Loading: {path}")
    with open(path) as f:
        raw = json.load(f)
    print(f"[+] {len(raw):,} raw records")

    processed = []
    skipped   = 0
    for i, r in enumerate(raw):
        dt = parse_dt(r.get("reported_datetime", ""))
        if not dt:
            skipped += 1

        row = {
            "id":                  i + 1,
            "incident_number":     r.get("incident_number", ""),
            "campus":              r.get("campus", "Unknown"),
            "campus_code":         r.get("campus_code", ""),
            "reported_datetime":   r.get("reported_datetime", ""),
            "occurred_datetime":   r.get("occurred_datetime", ""),
            "nature_of_incident":  r.get("nature_of_incident", ""),
            "offenses":            r.get("offenses", ""),
            "location":            r.get("location", ""),
            "date":                dt.strftime("%Y-%m-%d") if dt else "",
            "year":                dt.year  if dt else "",
            "month":               dt.month if dt else "",
            "month_name":          dt.strftime("%B") if dt else "",
            "hour":                dt.hour  if dt else "",
            "day_of_week":         dt.strftime("%A") if dt else "",
            "is_weekend":          "True" if dt and dt.weekday() >= 5 else "False",
            "time_bucket":         time_bucket(dt.hour if dt else None),
            "final_campus":        r.get("campus", "Unknown"),
        }
        processed.append(row)

    # Save master CSV
    os.makedirs(PROC_DIR, exist_ok=True)
    master_path = os.path.join(PROC_DIR, "incidents.csv")
    _write_csv(master_path, processed)
    print(f"[+] Master CSV: {master_path}  ({len(processed):,} rows)")

    # Per-campus CSVs
    campuses = set(r["campus"] for r in processed)
    for campus in sorted(campuses):
        rows  = [r for r in processed if r["campus"] == campus]
        safe  = re.sub(r"[^\w\-]", "_", campus)
        d     = os.path.join(CAMP_DIR, safe)
        os.makedirs(d, exist_ok=True)
        _write_csv(os.path.join(d, "incidents.csv"), rows)
        print(f"    {campus}: {len(rows):,} rows")

    # campuses.csv
    cc = Counter(r["campus"] for r in processed)
    rows_c = []
    for i, (name, cnt) in enumerate(sorted(cc.items()), 1):
        code = next((r["campus_code"] for r in processed if r["campus"] == name), "")
        rows_c.append({"campus_id": i, "campus_name": name,
                       "campus_code": code, "incident_count": cnt})
    _write_csv(os.path.join(PROC_DIR, "campuses.csv"), rows_c)

    # offense_types.csv
    off_counter = Counter()
    for r in processed:
        for o in re.split(r"(?<=[A-Z]{3})", r["offenses"]):
            o = o.strip()
            if o: off_counter[o] += 1
    _write_csv(os.path.join(PROC_DIR, "offense_types.csv"),
               [{"offense_type": k, "count": v}
                for k, v in off_counter.most_common()])

    print(f"\n[✓] Done. {len(campuses)} campuses, {skipped} rows w/o parseable date.")
    return processed


def _write_csv(path, rows):
    if not rows: return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=None)
    args = p.parse_args()
    process(args.input)
