import pandas as pd
from pathlib import Path

# ===============================
# CONFIGURATION
# ===============================
INPUT = Path("data/incidents.csv")
OUTPUT = Path("data/processed/incidents_with_time_features.csv")
INVALID_OUTPUT = Path("data/processed/incidents_with_invalid_time_values.csv")

TIMESTAMP_COL = "occurred_start"

# ===============================
# LOAD DATA
# ===============================
df = pd.read_csv(INPUT)

# ===============================
# VALIDATE TIMESTAMP COLUMN
# ===============================
if TIMESTAMP_COL not in df.columns:
    raise ValueError(f"Required column '{TIMESTAMP_COL}' not found.")

# Normalize raw values before parsing
df[TIMESTAMP_COL] = df[TIMESTAMP_COL].astype("string").str.strip()
df.loc[df[TIMESTAMP_COL] == "", TIMESTAMP_COL] = pd.NA

# Parse timestamps safely
df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL], errors="coerce")

# Separate valid and invalid rows
invalid_mask = df[TIMESTAMP_COL].isna()

if invalid_mask.any():
    invalid_rows = df.loc[invalid_mask].copy()
    INVALID_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    invalid_rows.to_csv(INVALID_OUTPUT, index=False)

    print(
        f"WARNING: {invalid_mask.sum()} row(s) have null or invalid "
        f"'{TIMESTAMP_COL}' values and were excluded from time feature engineering."
    )
    print(f"Invalid rows written to: {INVALID_OUTPUT}")

    # Keep only valid rows for feature engineering
    df = df.loc[~invalid_mask].copy()

if df.empty:
    raise ValueError(
        f"All rows contain invalid '{TIMESTAMP_COL}' values. "
        "Cannot generate time-based features."
    )

# ===============================
# TIME DERIVED FEATURES
# ===============================
df["date"] = df[TIMESTAMP_COL].dt.date
df["year"] = df[TIMESTAMP_COL].dt.year
df["month"] = df[TIMESTAMP_COL].dt.month
df["month_name"] = df[TIMESTAMP_COL].dt.month_name()

df["hour"] = df[TIMESTAMP_COL].dt.hour
df["day_of_week"] = df[TIMESTAMP_COL].dt.day_name()
df["is_weekend"] = df["day_of_week"].isin(["Saturday", "Sunday"])

# ===============================
# TIME BUCKETS
# ===============================
def time_bucket(hour: int) -> str:
    if hour >= 22 or hour <= 4:
        return "Late Night"
    elif 5 <= hour <= 11:
        return "Morning"
    elif 12 <= hour <= 16:
        return "Afternoon"
    else:
        return "Evening"


df["time_bucket"] = df["hour"].apply(time_bucket)

# ===============================
# WRITE OUTPUT
# ===============================
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT, index=False)

print("Time-derived feature engineering complete")
print(f"Output written to: {OUTPUT}")
print(f"Records processed: {len(df)}")