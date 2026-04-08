import pandas as pd
from pathlib import Path

# ===============================
# CONFIG
# ===============================
INPUT = Path("data/incidents.csv")
OUTPUT = Path("data/campuses")
AUDIT_FILE = Path("data/relocation_audit.csv")

AMBIGUOUS = "ambiguous_locations"
OFF_OR_UNKNOWN = "off_or_unknown"

# ===============================
# INCIDENT PREFIX → CAMPUS
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
        # Residence halls & academic buildings (State College)
        "SIMMONS HALL", "SPROUL HALL", "OSMOND", "DEIKE",
        "PEGULA", "BEAVER STADIUM", "BRYCE JORDAN",
        "NITTANY LION INN", "HUB", "POLLACK", "ATHERTON",
        "CURTIN", "BURROWES", "COLLEGE AVE", "PARK AVE",
        "STADIUM WEST", "IST BUILDING"
    ],
    "York": ["PULLO CENTER"],
    "Shenango": ["SHARON HALL"],
    "Scranton": ["SCRANTON"],
    "DuBois": ["DUBOIS", "E DUBOIS AVE"],
    "Mont_Alto": ["MONT ALTO"],
    "Wilkes_Barre": ["WILKES", "WB"],
    "Abington": ["ABINGTON", "LIONS GATE", "WOODLAND"],
    "Altoona": ["ALTOONA", "JUNIATA GAP"],
    "Hershey": ["HERSHEY", "HMC", "MEDICAL", "HOSPITAL"],
}

AMBIGUOUS_MARKERS = {
    "PENNSYLVANIA AVE",
    "UNIVERSITY DR",
    "1ST AVE",
    "DEPOT ST",
    "RIDGE VIEW DR",
    "COLLEGE PL",
}

OFF_MARKERS = {
    "OFF CAMPUS",
    "UNKNOWN LOCATION",
    "UNKNOWN ON CAMPUS PROPERTY",
}

# ===============================
# LOAD
# ===============================
df = pd.read_csv(INPUT)

# ===============================
# HELPERS
# ===============================
def campus_from_incident(incident):
    if pd.isna(incident):
        return "unknown"
    for prefix, campus in CAMPUS_MAP.items():
        if str(incident).startswith(prefix):
            return campus
    return "unknown"


def campus_from_location(location):
    if pd.isna(location):
        return "unknown"

    loc = location.upper()

    for m in OFF_MARKERS:
        if m in loc:
            return OFF_OR_UNKNOWN

    for m in AMBIGUOUS_MARKERS:
        if m in loc:
            return AMBIGUOUS

    for campus, keys in LOCATION_KEYWORDS.items():
        for k in keys:
            if k in loc:
                return campus

    return "unknown"


# ===============================
# ASSIGN CAMPUSES
# ===============================
df["prefix_campus"] = df["incident_number"].apply(campus_from_incident)
df["location_campus"] = df["location"].apply(campus_from_location)

def resolve(row):
    if row.location_campus in {AMBIGUOUS, OFF_OR_UNKNOWN}:
        return row.location_campus
    if row.location_campus != "unknown":
        return row.location_campus
    return row.prefix_campus if row.prefix_campus != "unknown" else OFF_OR_UNKNOWN

df["final_campus"] = df.apply(resolve, axis=1)

# ===============================
# AUDIT
# ===============================
df[df.prefix_campus != df.final_campus].to_csv(AUDIT_FILE, index=False)

# ===============================
# WRITE OUTPUT
# ===============================
for campus, cdf in df.groupby("final_campus"):
    out = OUTPUT / campus
    out.mkdir(parents=True, exist_ok=True)
    cdf.drop(columns=["prefix_campus", "location_campus"]).to_csv(
        out / "incidents.csv", index=False
    )

print("Expanded PSU-aware preprocessing complete")
