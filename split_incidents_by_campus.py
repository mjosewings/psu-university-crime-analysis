import pandas as pd
from pathlib import Path

#===============================
# CONFIGURATION
#===============================

INPUT = "data/incidents.csv"
OUTPUT = Path("data/campuses")

CAMPUS_MAP = {
    "26AB": "Abington",
    "26UP": "University_Park",
    "26AL": "Altoona",
    "26ER": "Erie",
    "26BK": "Berks",
    "26BW": "Brandywine",
    "26HB": "Harrisburg",
    "26HN": "Hazleton",
    "26SL": "Schuylkill",
    "26LV": "Lehigh_Valley",
    "PSHI": "Hershey"
}

#====================
# LOAD THE DATA
#====================

df = pd.read_csv(INPUT)

#=======================================
# INFERS THE CAMPUS FROM INCIDENT NUMBER
#=======================================

def infer_campus(incident_number: str) -> str:
    if pd.isna(incident_number):
        return "unknown"

    for prefix, campus in CAMPUS_MAP.items():
        if incident_number.startswith(prefix):
            return campus

    return "other"

#==============================
# WRITE CAMPUS-SPECIFIC FILES
#=============================

df['campus'] = df['incident_number'].astype(str).apply(infer_campus)

#===========================
# WRITE CAMPUS-SPECIFIC FILES
#===========================

for campus, campus_df in df.groupby('campus'):
    campus_dir = OUTPUT / campus
    campus_dir.mkdir(parents=True, exist_ok=True)

    output_path = campus_dir / "incidents.csv"
    campus_df.to_csv(output_path, index=False)

    print(f"Wrote {len(campus_df):>5} {output_path}")