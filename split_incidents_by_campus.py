import pandas as pd
from pathlib import Path

# ===============================
# CONFIG
# ===============================

# Canonical, processed dataset (already cleaned and time-enhanced)
INPUT = Path("data/processed/incidents_with_time_features.csv")

# Output directory for per-campus datasets
OUTPUT = Path("data/campuses")

# Audit file for transparency
AUDIT_FILE = Path("data/relocation_audit.csv")

AMBIGUOUS = "ambiguous_locations"
OFF_OR_UNKNOWN = "off_or_unknown"

# ===============================
# INCIDENT PREFIX → CAMPUS MAP
# ===============================

CAMPUS_MAP = {
    "26UP": "University_Park",
    "26AB": "Abington",
    "26AL": "Altoona",
    "26ER": "Erie",
    "26BK": "Berks",
    "26BW": "Brandywine",
    "26HB": "Harrisburg",
    "26HN": "Hazleton",
    "26SL": "Schuylkill",
    "26LV": "Lehigh_Valley",
    "26YK": "York",
    "26SH": "Shenango",
    "26WS": "Scranton",
    "26DS": "DuBois",
    "26MA": "Mont_Alto",
    "26WB": "Wilkes_Barre",
    "PSHI": "Hershey",
}

# ===============================
# LOCATION KEYWORDS → CAMPUS
# ===============================

LOCATION_KEYWORDS = {
    "University_Park": [
        "SIMMONS HALL", "SPROUL HALL", "OSMOND", "DEIKE",
        "PEGULA", "BEAVER STADIUM", "BRYCE JORDAN",
        "NITTANY LION INN", "HUB", "POLLACK", "ATHERTON",
        "CURTIN", "BURROWES", "COLLEGE AVE", "PARK AVE",
        "STADIUM WEST", "IST BUILDING"
    ],
    "Abington": ["ABINGTON", "LIONS GATE", "WOODLAND"],
    "Altoona": ["ALTOONA"],
    "Hershey": ["HERSHEY", "HMC", "MEDICAL", "HOSPITAL"],
}

AMBIGUOUS_MARKERS = {
    "PENNSYLVANIA AVE", "UNIVERSITY DR",
    "1ST AVE", "DEPOT ST", "RIDGE VIEW DR", "COLLEGE PL"
}

OFF_MARKERS = {
    "OFF CAMPUS",
    "UNKNOWN LOCATION",
    "UNKNOWN ON CAMPUS PROPERTY",
}

# ===============================
# LOAD DATA
# ===============================

df = pd.read_csv(INPUT)

# ===============================
# HELPERS
# ===============================

def campus_from_incident(incident_number):
    if pd.isna(incident_number):
        return "unknown"
    for prefix, campus in CAMPUS_MAP.items():
        if str(incident_number).startswith(prefix):
            return campus
    return "unknown"


def campus_from_location(location):
    if pd.isna(location):
        return "unknown"
    location_upper = location.upper()

    if any(marker in location_upper for marker in OFF_MARKERS):
        return OFF_OR_UNKNOWN

    if any(marker in location_upper for marker in AMBIGUOUS_MARKERS):
        return AMBIGUOUS

    for campus, keywords in LOCATION_KEYWORDS.items():
        if any(keyword in location_upper for keyword in keywords):
            return campus

    return "unknown"


# ===============================
# CAMPUS ASSIGNMENT
# ===============================

df["prefix_campus"] = df["incident_number"].apply(campus_from_incident)
df["location_campus"] = df["location"].apply(campus_from_location)


def resolve_final_campus(row):
    if row.location_campus in {AMBIGUOUS, OFF_OR_UNKNOWN}:
        return row.location_campus
    if row.location_campus != "unknown":
        return row.location_campus
    return row.prefix_campus if row.prefix_campus != "unknown" else OFF_OR_UNKNOWN


df["final_campus"] = df.apply(resolve_final_campus, axis=1)

# ===============================
# AUDIT
# ===============================

df[df["prefix_campus"] != df["final_campus"]].to_csv(
    AUDIT_FILE, index=False
)

# ===============================
# SPLIT & WRITE (PRESERVE ALL COLUMNS)
# ===============================

for campus, campus_df in df.groupby("final_campus"):
    out_dir = OUTPUT / campus
    out_dir.mkdir(parents=True, exist_ok=True)

    campus_df.drop(
        columns=["prefix_campus", "location_campus"],
        errors="ignore"
    ).to_csv(out_dir / "incidents.csv", index=False)

print("Campus splitting complete (time features preserved)")
