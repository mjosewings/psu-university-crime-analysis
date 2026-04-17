import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False

# ===============================
# CONFIG
# ===============================
BASE_DIR = Path("data/campuses")
OUTDIR = Path("outputs/plots")
OUTDIR.mkdir(parents=True, exist_ok=True)

WEEKDAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# ===============================
# GET ALL CAMPUSES
# ===============================
campus_dirs = [p for p in BASE_DIR.iterdir() if p.is_dir()]

if not campus_dirs:
    raise ValueError("No campus directories found inside data/campuses/")

# ===============================
# PROCESS EACH CAMPUS
# ===============================
for campus_path in campus_dirs:
    campus_name = campus_path.name
    input_file = campus_path / "incidents.csv"

    if not input_file.exists():
        print(f"Skipping {campus_name}: no incidents.csv found")
        continue

    print(f"Processing campus: {campus_name}")

    df = pd.read_csv(input_file)

    # ===============================
    # VALIDATION
    # ===============================
    required_cols = {"hour", "day_of_week"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"{campus_name}: Missing columns {missing}")

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df["day_of_week"] = df["day_of_week"].astype(str)

    if df["hour"].isna().any():
        raise ValueError(f"{campus_name}: invalid hour values found")

    campus_out = OUTDIR / campus_name
    campus_out.mkdir(parents=True, exist_ok=True)

    # ===============================
    # PLOT 1: Hour of Day
    # ===============================
    hour_counts = df["hour"].value_counts().reindex(range(24), fill_value=0)

    plt.figure()
    plt.bar(hour_counts.index, hour_counts.values)
    plt.title(f"{campus_name} — Incidents by Hour of Day")
    plt.xlabel("Hour of Day (0–23)")
    plt.ylabel("Number of Incidents")
    plt.xticks(range(24))
    plt.tight_layout()
    plt.savefig(campus_out / f"{campus_name.lower()}_incidents_by_hour.png", dpi=200)
    plt.close()

    # ===============================
    # PLOT 2: Day of Week
    # ===============================
    dow_counts = df["day_of_week"].value_counts().reindex(WEEKDAY_ORDER, fill_value=0)

    plt.figure()
    plt.bar(dow_counts.index, dow_counts.values)
    plt.title(f"{campus_name} — Incidents by Day of Week")
    plt.xlabel("Day of Week")
    plt.ylabel("Number of Incidents")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(campus_out / f"{campus_name.lower()}_incidents_by_day_of_week.png", dpi=200)
    plt.close()

    # ===============================
    # PLOT 3: HEATMAP
    # ===============================
    value_col = "incident_number" if "incident_number" in df.columns else "hour"

    pivot = pd.pivot_table(
        df,
        index="day_of_week",
        columns="hour",
        values=value_col,
        aggfunc="count",
        fill_value=0
    ).reindex(WEEKDAY_ORDER)

    plt.figure(figsize=(12, 4))

    if SEABORN_AVAILABLE:
        sns.heatmap(pivot, cmap="Blues", cbar_kws={"label": "Incident Count"})
        plt.title(f"{campus_name} — Heatmap (Day × Hour)")
        plt.xlabel("Hour")
        plt.ylabel("Day of Week")
    else:
        plt.imshow(pivot.values, aspect="auto")
        plt.colorbar(label="Incident Count")
        plt.title(f"{campus_name} — Heatmap (Day × Hour)")
        plt.xlabel("Hour")
        plt.ylabel("Day of Week")
        plt.xticks(range(24))
        plt.yticks(range(len(WEEKDAY_ORDER)), WEEKDAY_ORDER)

    plt.tight_layout()
    plt.savefig(campus_out / f"{campus_name.lower()}_heatmap.png", dpi=200)
    plt.close()

    # ===============================
    # PLOT 4: TIME BUCKET
    # ===============================
    if "time_bucket" in df.columns:
        bucket_counts = df["time_bucket"].value_counts().reindex(
            ["Morning", "Afternoon", "Evening", "Late Night"],
            fill_value=0
        )

        plt.figure()
        plt.bar(bucket_counts.index, bucket_counts.values)
        plt.title(f"{campus_name} — Incidents by Time Bucket")
        plt.xlabel("Time of Day")
        plt.ylabel("Number of Incidents")
        plt.tight_layout()
        plt.savefig(campus_out / f"{campus_name.lower()}_time_bucket.png", dpi=200)
        plt.close()

    # ===============================
    # PLOT 5: TOP INCIDENT TYPES
    # ===============================
    if "nature_of_incident" in df.columns:
        top_types = df["nature_of_incident"].value_counts().head(10)

        plt.figure(figsize=(8, 4))
        plt.barh(top_types.index[::-1], top_types.values[::-1])
        plt.title(f"{campus_name} — Top Incident Types")
        plt.xlabel("Count")
        plt.ylabel("Type")
        plt.tight_layout()
        plt.savefig(campus_out / f"{campus_name.lower()}_top_incident_types.png", dpi=200)
        plt.close()

    # ===============================
    # PLOT 6: TYPE × TIME
    # ===============================
    if {"nature_of_incident", "time_bucket"}.issubset(df.columns):
        top_categories = df["nature_of_incident"].value_counts().head(5).index
        subset = df[df["nature_of_incident"].isin(top_categories)]

        pivot_tb = pd.pivot_table(
            subset,
            index="time_bucket",
            columns="nature_of_incident",
            aggfunc="size",
            fill_value=0
        ).reindex(["Morning", "Afternoon", "Evening", "Late Night"])

        pivot_tb.plot(kind="bar", stacked=True, figsize=(10, 5))

        plt.title(f"{campus_name} — Incident Types by Time Bucket")
        plt.xlabel("Time of Day")
        plt.ylabel("Number of Incidents")
        plt.legend(bbox_to_anchor=(1.02, 1))
        plt.tight_layout()
        plt.savefig(campus_out / f"{campus_name.lower()}_type_by_time.png", dpi=200)
        plt.close()

print("Done! All campus plots saved to:", OUTDIR.resolve())