"""
Penn State Daily Crime Log Scraper
==================================

Scrapes the public Daily Crime Log from the Penn State University Police
website for one or more campuses and builds a SQLite database plus a JSON
export of the raw records.

By default, the scraper pulls approximately the **last three years** of
records for each campus. This provides enough historical depth for trend
analysis while remaining tractable for periodic reruns. You can override the
window using the ``--days`` flag.

Example usage
-------------

    # Scrape last 3 years for all campuses (default)
    python psu_crime_scraper.py

    # Scrape last 60 days
    python psu_crime_scraper.py --days 60

    # Scrape last 3 years but only for Penn State Abington
    python psu_crime_scraper.py --campus "Abington"

    # Enable verbose debug logging and save HTML snapshots
    python psu_crime_scraper.py --debug

All data is sourced from official public Daily Crime Log records and is
aggregated for campus-level analysis. No personally identifiable information
(PII) is stored or analyzed.
"""

import sqlite3
import re
import json
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────

BASE_URL = "https://www.police.psu.edu/daily-crime-log"
DB_PATH = "psu_crime_log.db"
DEBUG_HTML_DIR = Path("debug_html")

# Default time window when the user does not specify --days explicitly.
# Three years is large enough for temporal trends but still manageable to
# scrape on a typical laptop.
DEFAULT_DAYS = 365 * 3

# Safety cap to avoid accidentally scraping an extremely long history in
# a single run (which could stress both the client and the remote site).
MAX_DAYS = 365 * 10

CAMPUSES = {
    "Univ Park":     "University Park",
    "Abington":      "Abington",
    "Altoona":       "Altoona",
    "Beaver":        "Beaver",
    "Behrend":       "Erie (Behrend)",
    "Berks":         "Berks",
    "Brandywine":    "Brandywine",
    "Dickinson Law": "Dickinson Law",
    "DuBois":        "DuBois",
    "Fayette":       "Fayette",
    "Grtr Algny":    "Greater Allegheny",
    "Grt Valley":    "Great Valley",
    "Harrisburg":    "Harrisburg",
    "Hazleton":      "Hazleton",
    "Hershey":       "Hershey",
    "Lehigh Val":    "Lehigh Valley",
    "Mont Alto":     "Mont Alto",
    "New Ken":       "New Kensington",
    "Schuylkill":    "Schuylkill",
    "Shenango":      "Shenango",
    "Wlks-Barre":    "Wilkes-Barre",
    "Wrthn Scrn":    "Worthington Scranton",
    "York":          "York",
}

# Campus code mapping for incident numbers
CAMPUS_CODES = {
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
    # Temporary/alternate codes seen in incident numbers
    "HN": "Hershey",
    "ER": "Erie (Behrend)",
    "BKT": "Beaver",
    "SL": "Schuylkill",
    "DS": "DuBois",
    "FE": "Fayette",
    "ABT": "Abington",
    "PSHI": "University Park",  # Penn State Hershey incidents
}

# Reverse mapping
CAMPUS_NAME_TO_CODE = {v: k for k, v in CAMPUS_CODES.items()}

# ── Logging Setup ──────────────────────────────────────────────────────────

def setup_logging(debug=False):
    """Configure logging based on debug flag."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(__name__)

logger = logging.getLogger(__name__)

# ── Parsing ────────────────────────────────────────────────────────────────

def save_debug_html(html: str, campus: str, page: int):
    """Save raw HTML to disk for debugging when the site structure changes."""
    DEBUG_HTML_DIR.mkdir(exist_ok=True)
    filepath = DEBUG_HTML_DIR / f"{campus.replace(' ', '_')}_page{page}.html"
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.debug(f"Saved debug HTML to {filepath}")

def parse_incidents(html: str, campus_label: str, debug: bool = False) -> List[Dict]:
    """
    Parse incident records from the crime log HTML page.

    The page structure uses .views-row for each incident, with structured HTML fields.
    Fields are in divs with class "field__item" under labeled containers.
    """
    soup = BeautifulSoup(html, "html.parser")
    incidents = []

    # Each incident is rendered as a `.views-row` block in the PSU site.
    # If this selector stops matching, the upstream HTML likely changed.
    blocks = soup.select(".views-row")

    if not blocks:
        logger.warning(f"No incident blocks found for {campus_label}")
        if debug:
            logger.debug("HTML preview:")
            logger.debug(soup.prettify()[:2000])
        return incidents

    for idx, block in enumerate(blocks):
        incident = {
            "campus": campus_label,
            "incident_number": "",
            "campus_code": CAMPUS_NAME_TO_CODE.get(campus_label, ""),
            "reported_datetime": "",
            "occurred_datetime": "",
            "nature_of_incident": "",
            "offenses": "",
            "location": "",
        }

        try:
            # Extract Incident Number from h2 or title field
            incident_elem = block.select_one("span.field--name-title")
            if incident_elem:
                incident["incident_number"] = incident_elem.get_text(strip=True)
                # Extract campus code from incident number (e.g., "24UP12345" -> "UP")
                code_match = re.match(r'\d{2}([A-Z]{2,3})', incident["incident_number"])
                if code_match:
                    incident["campus_code"] = code_match.group(1)

            # Extract Reported datetime
            reported_elem = block.select_one("div.field--name-field-reported .field__item")
            if reported_elem:
                incident["reported_datetime"] = reported_elem.get_text(strip=True)

            # Extract Occurred datetime (may be a range)
            occurred_elem = block.select_one("div.field--name-field-occurred .field__item")
            if occurred_elem:
                incident["occurred_datetime"] = occurred_elem.get_text(strip=True)

            # Extract Nature of Incident
            nature_elem = block.select_one("div.field--name-field-nature-of-incident1 .field__item")
            if nature_elem:
                incident["nature_of_incident"] = nature_elem.get_text(strip=True)

            # Extract Offenses
            offenses_elem = block.select_one("div.field--name-field-offenses1 .field__item")
            if offenses_elem:
                incident["offenses"] = offenses_elem.get_text(strip=True)

            # Extract Location
            location_elem = block.select_one("div.field--name-field-location .field__item")
            if location_elem:
                incident["location"] = location_elem.get_text(strip=True)

            if debug and idx == 0:
                logger.debug(f"First incident parsed:")
                logger.debug(f"  Number: {incident['incident_number']}")
                logger.debug(f"  Reported: {incident['reported_datetime']}")
                logger.debug(f"  Nature: {incident['nature_of_incident']}")

            # Only add if we have at least some data
            if any([
                incident.get("incident_number"),
                incident.get("reported_datetime"),
                incident.get("nature_of_incident")
            ]):
                incidents.append(incident)
            else:
                logger.debug(f"Skipping empty incident block {idx}")

        except Exception as e:
            logger.debug(f"Error parsing incident block {idx}: {e}")
            continue

    return incidents

# ── Scraping ───────────────────────────────────────────────────────────────

def scrape_campus(campus_filter: str, campus_label: str,
                  start_date: Optional[str] = None, end_date: Optional[str] = None,
                  session: Optional[requests.Session] = None,
                  max_pages: int = 100,
                  debug: bool = False) -> List[Dict]:
    """
    Scrape all available pages for a single campus within an optional date range.

    Parameters
    ----------
    campus_filter:
        Short label used by the PSU site in the ``campus`` query parameter
        (e.g. ``"Univ Park"``).
    campus_label:
        Human-readable campus name stored in the record payload.
    start_date, end_date:
        Inclusive date range in ``MM/DD/YYYY`` format. If either is ``None``,
        the PSU site's default behaviour is used.
    session:
        Optional shared :class:`requests.Session` for connection reuse.
    max_pages:
        Upper bound on pagination iterations as a safety net.
    debug:
        When ``True``, emit additional logging and save HTML snapshots.
    """

    s = session or requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })

    all_incidents = []
    page = 0
    consecutive_empty = 0
    max_consecutive_empty = 3

    while page < max_pages:
        params = {"campus": campus_filter, "page": page}
        if start_date:
            params["field_reported_date_value[min]"] = start_date
        if end_date:
            params["field_reported_date_value[max]"] = end_date

        try:
            logger.info(f"  Fetching page {page}...")
            resp = s.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()

            if debug:
                save_debug_html(resp.text, campus_label, page)

            # Check if we got a valid response
            if len(resp.text) < 500:
                logger.warning(f"  Suspiciously short response ({len(resp.text)} bytes)")
                break

        except requests.exceptions.Timeout:
            logger.error(f"  Timeout fetching {campus_label} page {page}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"  Error fetching {campus_label} page {page}: {e}")
            break

        incidents = parse_incidents(resp.text, campus_label, debug=debug)

        if not incidents:
            consecutive_empty += 1
            logger.info(f"  Page {page}: No incidents found (empty count: {consecutive_empty})")
            if consecutive_empty >= max_consecutive_empty:
                logger.info(f"  Stopping after {consecutive_empty} consecutive empty pages")
                break
        else:
            consecutive_empty = 0
            all_incidents.extend(incidents)
            logger.info(f"  Page {page}: Found {len(incidents)} incidents (total: {len(all_incidents)})")

            if debug and incidents:
                logger.debug(f"  Sample incident: {json.dumps(incidents[0], indent=2)}")

        page += 1
        time.sleep(1.0)  # Be nice to the server

    return all_incidents

def scrape_all_campuses(days_back: int = DEFAULT_DAYS, specific_campus: Optional[str] = None,
                        debug: bool = False) -> List[Dict]:
    """
    Scrape the last N days for ALL campuses (or a specific one).

    The default window (:data:`DEFAULT_DAYS`) is roughly three years of
    history, which is a good balance between temporal coverage and runtime.
    """
    end_dt    = datetime.now()
    start_dt  = end_dt - timedelta(days=days_back)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    logger.info(f"Date range: {start_str} to {end_str}")

    session     = requests.Session()
    all_records = []

    # Filter to specific campus if requested
    campuses_to_scrape = CAMPUSES.items()
    if specific_campus:
        # Find matching campus
        matched = False
        for filter_code, label in CAMPUSES.items():
            if specific_campus.lower() in label.lower():
                campuses_to_scrape = [(filter_code, label)]
                matched = True
                break
        if not matched:
            logger.error(f"Campus '{specific_campus}' not found!")
            return []

    for filter_code, label in campuses_to_scrape:
        logger.info(f"\n🏫 Scraping: {label} ({filter_code})")
        records = scrape_campus(filter_code, label, start_str, end_str, session, debug=debug)
        logger.info(f"  ✅ {len(records)} total incidents")
        all_records.extend(records)

        # Small delay between campuses
        time.sleep(2.0)

    return all_records

# ── Database ───────────────────────────────────────────────────────────────

CREATE_CAMPUSES_SQL = """
CREATE TABLE IF NOT EXISTS campuses (
    campus_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    campus_code  TEXT NOT NULL UNIQUE,
    campus_name  TEXT NOT NULL
);
"""

CREATE_INCIDENTS_SQL = """
CREATE TABLE IF NOT EXISTS incidents (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_number     TEXT UNIQUE,
    campus_id           INTEGER REFERENCES campuses(campus_id),
    reported_datetime   TEXT,
    occurred_start      TEXT,
    occurred_end        TEXT,
    nature_of_incident  TEXT,
    location            TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_OFFENSES_SQL = """
CREATE TABLE IF NOT EXISTS offense_types (
    offense_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    offense_code TEXT NOT NULL UNIQUE,
    description  TEXT
);
"""

CREATE_INCIDENT_OFFENSES_SQL = """
CREATE TABLE IF NOT EXISTS incident_offenses (
    incident_id INTEGER REFERENCES incidents(id),
    offense_id  INTEGER REFERENCES offense_types(offense_id),
    PRIMARY KEY (incident_id, offense_id)
);
"""

CREATE_VIEWS_SQL = [
    """
    CREATE VIEW IF NOT EXISTS v_incidents_full AS
    SELECT
        i.id,
        i.incident_number,
        c.campus_name AS campus,
        i.reported_datetime,
        i.occurred_start,
        i.occurred_end,
        i.nature_of_incident,
        i.location,
        (
            SELECT GROUP_CONCAT(ot.offense_code, ' | ')
            FROM incident_offenses io
            JOIN offense_types ot ON ot.offense_id = io.offense_id
            WHERE io.incident_id = i.id
        ) AS offenses
    FROM incidents i
    JOIN campuses c ON i.campus_id = c.campus_id;
    """,
    """
    CREATE VIEW IF NOT EXISTS v_incidents_by_campus AS
    SELECT
        c.campus_name,
        COUNT(*)                 AS total_incidents,
        MIN(i.reported_datetime) AS earliest_report,
        MAX(i.reported_datetime) AS latest_report
    FROM incidents i
    JOIN campuses c ON i.campus_id = c.campus_id
    GROUP BY c.campus_name
    ORDER BY total_incidents DESC;
    """,
    """
    CREATE VIEW IF NOT EXISTS v_top_offenses AS
    SELECT
        ot.offense_code,
        COUNT(*) AS frequency
    FROM incident_offenses io
    JOIN offense_types ot ON ot.offense_id = io.offense_id
    GROUP BY ot.offense_code
    ORDER BY frequency DESC;
    """,
]

CAMPUS_SEED = [
    (code, name) for code, name in CAMPUS_CODES.items()
]

def build_database(records: List[Dict], db_path: str = DB_PATH) -> sqlite3.Connection:
    """Create the SQLite database and insert all records."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Create tables
    cur.execute(CREATE_CAMPUSES_SQL)
    cur.execute(CREATE_INCIDENTS_SQL)
    cur.execute(CREATE_OFFENSES_SQL)
    cur.execute(CREATE_INCIDENT_OFFENSES_SQL)
    for view_sql in CREATE_VIEWS_SQL:
        cur.execute(view_sql)

    # Seed campuses
    cur.executemany(
        "INSERT OR IGNORE INTO campuses (campus_code, campus_name) VALUES (?,?)",
        CAMPUS_SEED
    )

    # Build campus lookup
    cur.execute("SELECT campus_code, campus_id FROM campuses")
    campus_map = dict(cur.fetchall())

    inserted = 0
    updated = 0
    skipped = 0

    for rec in records:
        code = rec.get("campus_code", "")
        campus_id = campus_map.get(code)

        if not campus_id:
            logger.warning(f"  Unknown campus code: {code} for campus {rec.get('campus')}")
            # Try to get by name
            campus_name = rec.get("campus", "")
            code = CAMPUS_NAME_TO_CODE.get(campus_name)
            campus_id = campus_map.get(code) if code else None

            if not campus_id:
                logger.warning(f"  Skipping record, cannot determine campus")
                skipped += 1
                continue

        # Parse occurred datetime
        occ       = rec.get("occurred_datetime", "")
        occ_parts = [p.strip() for p in occ.split(" to ")] if occ else []
        occ_start = occ_parts[0] if occ_parts else ""
        occ_end   = occ_parts[1] if len(occ_parts) > 1 else ""

        incident_number = rec.get("incident_number", "")

        try:
            # Check if incident already exists
            if incident_number:
                cur.execute(
                    "SELECT id FROM incidents WHERE incident_number = ?",
                    (incident_number,)
                )
                existing = cur.fetchone()

                if existing:
                    logger.debug(f"  Incident {incident_number} already exists, skipping")
                    skipped += 1
                    continue

            # Insert incident
            cur.execute(
                """INSERT INTO incidents
                   (incident_number, campus_id, reported_datetime,
                    occurred_start, occurred_end,
                    nature_of_incident, location)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    incident_number or None,
                    campus_id,
                    rec.get("reported_datetime", ""),
                    occ_start,
                    occ_end,
                    rec.get("nature_of_incident", ""),
                    rec.get("location", ""),
                )
            )
            inc_id = cur.lastrowid

            # Insert offenses
            offense_text = rec.get("offenses", "")
            if offense_text:
                # Split by newlines and clean
                for raw_offense in offense_text.split("\n"):
                    code_str = raw_offense.strip()
                    if not code_str or len(code_str) > 200:
                        continue

                    cur.execute(
                        "INSERT OR IGNORE INTO offense_types (offense_code) VALUES (?)",
                        (code_str,)
                    )
                    cur.execute(
                        "SELECT offense_id FROM offense_types WHERE offense_code=?",
                        (code_str,)
                    )
                    off_row = cur.fetchone()
                    if off_row:
                        off_id = off_row[0]
                        cur.execute(
                            "INSERT OR IGNORE INTO incident_offenses VALUES (?,?)",
                            (inc_id, off_id)
                        )

            inserted += 1

        except sqlite3.IntegrityError as e:
            logger.debug(f"  Duplicate incident: {incident_number}")
            skipped += 1
        except Exception as e:
            logger.error(f"  ⚠️  DB insert error for {incident_number}: {e}")
            logger.debug(f"  Problematic record: {json.dumps(rec, indent=2)}")
            skipped += 1

    con.commit()
    logger.info(f"\n✅ Inserted: {inserted} | Skipped: {skipped}")
    return con

# ── Summary ───────────────────────────────────────────────────────────────

def print_summary(con: sqlite3.Connection):
    """Print database statistics."""
    cur = con.cursor()

    print("\n" + "="*70)
    print(" DATABASE SUMMARY")
    print("="*70)

    cur.execute("SELECT COUNT(*) FROM incidents")
    total = cur.fetchone()[0]
    print(f"  Total incidents:    {total}")

    if total == 0:
        print("\n  ⚠️  No incidents found in database!")
        print("  This could mean:")
        print("    - The website structure has changed")
        print("    - There are no incidents in the selected date range")
        print("    - Network issues prevented scraping")
        print("\n  Try running with --debug flag for more information")
        print("="*70)
        return

    cur.execute("SELECT COUNT(*) FROM offense_types")
    print(f"  Unique offenses:    {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(DISTINCT campus_id) FROM incidents")
    print(f"  Campuses with data: {cur.fetchone()[0]}")

    print("\n── Incidents by Campus " + "─"*45)
    cur.execute("SELECT * FROM v_incidents_by_campus LIMIT 15")
    rows = cur.fetchall()

    if rows:
        print(f"  {'Campus':<25} {'Total':>6} {'Latest':>12}")
        print("  " + "-"*45)
        for campus, total, _, latest in rows:
            latest_str = (latest or '')[:10] if latest else 'N/A'
            print(f"  {campus:<25} {total:>6} {latest_str:>12}")
    else:
        print("  No data available")

    print("\n── Top 10 Offense Types " + "─"*42)
    cur.execute("SELECT * FROM v_top_offenses LIMIT 10")
    rows = cur.fetchall()

    if rows:
        for offense, freq in rows:
            print(f"  {offense:<50} {freq:>4}x")
    else:
        print("  No offense data available")

    print("\n── Recent Incidents " + "─"*48)
    cur.execute("""
        SELECT incident_number, campus, nature_of_incident, reported_datetime
        FROM v_incidents_full
        ORDER BY reported_datetime DESC
        LIMIT 5
    """)
    rows = cur.fetchall()

    if rows:
        for inc_num, campus, nature, reported in rows:
            print(f"\n  {inc_num or 'N/A'} - {campus}")
            print(f"  {nature[:65]}")
            print(f"  Reported: {reported}")
    else:
        print("  No recent incidents")

    print("\n" + "="*70)

