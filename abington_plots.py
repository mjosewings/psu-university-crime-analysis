import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# seaborn is optional but makes the heatmap simpler
try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False

# ===============================
# CONFIG
# ===============================
INPUT = Path("data/campuses/Abington/incidents.csv")
OUTDIR = Path("outputs/plots")
OUTDIR.mkdir(parents=True, exist_ok=True)

# For consistent weekday ordering
WEEKDAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# ===============================
# LOAD
# ===============================
df = pd.read_csv(INPUT)

# ===============================
# BASIC VALIDATION
# ===============================
required_cols = {"hour", "day_of_week"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(
        f"Missing required columns: {missing}. "
        "Make sure you ran time-derived feature engineering and then re-split into campuses."
    )

# Ensure correct types
df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
df["day_of_week"] = df["day_of_week"].astype(str)

if df["hour"].isna().any():
    raise ValueError("Found non-numeric or missing hour values. Check occurred_start parsing in feature engineering.")

# ===============================
# PLOT 1: Incidents by Hour of Day
# ===============================
hour_counts = df["hour"].value_counts().reindex(range(24), fill_value=0)

plt.figure()
plt.bar(hour_counts.index, hour_counts.values)
plt.title("Penn State Abington — Incidents by Hour of Day")
plt.xlabel("Hour of Day (0–23)")
plt.ylabel("Number of Incidents")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig(OUTDIR / "abington_incidents_by_hour.png", dpi=200)
plt.close()

# ===============================
# PLOT 2: Incidents by Day of Week
# ===============================
dow_counts = df["day_of_week"].value_counts()
dow_counts = dow_counts.reindex(WEEKDAY_ORDER, fill_value=0)

plt.figure()
plt.bar(dow_counts.index, dow_counts.values)
plt.title("Penn State Abington — Incidents by Day of Week")
plt.xlabel("Day of Week")
plt.ylabel("Number of Incidents")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig(OUTDIR / "abington_incidents_by_day_of_week.png", dpi=200)
plt.close()

# ===============================
# PLOT 3: Heatmap (Day of Week × Hour)
# ===============================
pivot = pd.pivot_table(
    df,
    index="day_of_week",
    columns="hour",
    values="incident_number" if "incident_number" in df.columns else "hour",
    aggfunc="count",
    fill_value=0
).reindex(WEEKDAY_ORDER)

plt.figure(figsize=(12, 4))

if SEABORN_AVAILABLE:
    sns.heatmap(pivot, cmap="Blues", cbar_kws={"label": "Incident Count"})
    plt.title("Penn State Abington — Heatmap of Incidents (Day of Week × Hour)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Day of Week")
    plt.tight_layout()
else:
    # Fallback without seaborn
    plt.imshow(pivot.values, aspect="auto")
    plt.colorbar(label="Incident Count")
    plt.title("Penn State Abington — Heatmap of Incidents (Day of Week × Hour)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Day of Week")
    plt.xticks(ticks=range(24), labels=range(24))
    plt.yticks(ticks=range(len(WEEKDAY_ORDER)), labels=WEEKDAY_ORDER)
    plt.tight_layout()

plt.savefig(OUTDIR / "abington_heatmap_day_hour.png", dpi=200)
plt.close()


# ===============================
# PLOT 4: Incidents by Time Bucket
# ===============================

if "time_bucket" in df.columns:
    bucket_counts = df["time_bucket"].value_counts().reindex(
        ["Morning", "Afternoon", "Evening", "Late Night"],
        fill_value=0
    )

    plt.figure()
    plt.bar(bucket_counts.index, bucket_counts.values)
    plt.title("Penn State Abington — Incidents by Time of Day")
    plt.xlabel("Time of Day")
    plt.ylabel("Number of Incidents")
    plt.tight_layout()
    plt.savefig(OUTDIR / "abington_incidents_by_time_bucket.png", dpi=200)
    plt.close()

# ===============================
# PLOT 5: Top Incident Types
# ===============================

if "nature_of_incident" in df.columns:
    top_types = (
        df["nature_of_incident"]
        .value_counts()
        .head(10)
    )

    plt.figure(figsize=(8, 4))
    plt.barh(top_types.index[::-1], top_types.values[::-1])
    plt.title("Penn State Abington — Most Common Incident Types")
    plt.xlabel("Number of Incidents")
    plt.ylabel("Incident Type")
    plt.tight_layout()
    plt.savefig(OUTDIR / "abington_top_incident_types.png", dpi=200)
    plt.close()

# ===============================
# PLOT 6: Incident Type × Time Bucket
# ===============================

if {"nature_of_incident", "time_bucket"}.issubset(df.columns):
    top_categories = (
        df["nature_of_incident"]
        .value_counts()
        .head(5)
        .index
    )

    subset = df[df["nature_of_incident"].isin(top_categories)]

    pivot_tb = pd.pivot_table(
        subset,
        index="time_bucket",
        columns="nature_of_incident",
        aggfunc="size",
        fill_value=0
    ).reindex(["Morning", "Afternoon", "Evening", "Late Night"])

    pivot_tb.plot(
        kind="bar",
        stacked=True,
        figsize=(10, 5)
    )

    plt.title("Penn State Abington — Incident Types by Time of Day")
    plt.xlabel("Time of Day")
    plt.ylabel("Number of Incidents")
    plt.legend(title="Incident Type", bbox_to_anchor=(1.02, 1))
    plt.tight_layout()
    plt.savefig(OUTDIR / "abington_incident_types_by_time_bucket.png", dpi=200)
    plt.close()



print("Done! Plots saved to:", OUTDIR.resolve())
