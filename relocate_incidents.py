import pandas as pd
from pathlib import Path

#=====================
# CONFIGURATION
#================

INPUT = Path("data/incidents.csv")
OUTPUT = Path("data/campuses")
AUDIT_FILE = Path("data/relocation_audit.csv")
UNKNOWN_CAMPUS = Path("data/unknown_locations.csv")
