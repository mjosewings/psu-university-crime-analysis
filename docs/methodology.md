# **Methodology**

## **Overview**

This project constructs a reproducible data pipeline for collecting, cleaning, and organizing Penn State University crime log incidents into campus-specific datasets. The primary goal is to ensure that each incident is accurately associated with the correct Penn State campus while avoiding misclassification caused by ambiguous or incomplete location data.

The methodology emphasizes:

*   Accuracy over completeness
*   Transparency of classification decisions
*   Preservation of ambiguous or out-of-scope data for auditing

***

## **Data Sources**

The primary data source for this project is the Penn State Daily Crime Log, which publishes incident-level safety reports recorded by Penn State Police and affiliated units.

The raw data is stored in two formats:

*   A SQLite database (`psu_crime_log.db`)
*   A JSON export (`psu_crime_log_records.json`)

These raw data sources are treated as immutable and are never modified directly. All transformations occur downstream in the preprocessing stage.

---

## **Data Pipeline Architecture**

The pipeline follows a staged design:

1.  **Data Collection**  
    Crime log records are scraped and stored in raw form.

2.  **Normalization and Cleaning**  
    Incident records are cleaned, standardized, and exported to CSV format. Campus identifiers, offense types, and relationships between incidents and offenses are normalized.

3.  **Campus Assignment and Validation**  
    Incidents are assigned to campuses using a multi-step resolution strategy based on incident metadata and location text.

4.  **Campus-Specific Dataset Generation**  
    Incidents are split into campus-specific directories for ease of analysis.

Each stage produces explicit outputs that can be inspected independently.

---

## **Campus Assignment Strategy**

Assigning incidents to the correct Penn State campus is the most critical and error-prone step. The methodology uses **layered inference**, resolving campus assignment in the following priority order.

### **1. Location-Based Assignment (Authoritative)**

If the location field contains the name of a known Penn State building, residence hall, athletic facility, or campus-specific landmark, that information is treated as authoritative.

Examples:

*   `Simmons Hall`, `Sproul Hall`, `Osmond Lab` → **University Park**
*   `Pullo Center` → **Penn State York**
*   `Sharon Hall` → **Penn State Shenango**

Building-to-campus mappings were verified using official Penn State campus maps and facility listings.

Importantly, locations within **State College Borough** that reference Penn State facilities are assigned to **University Park**, as University Park is the flagship campus encompassing those facilities.

---

### **2. Incident Number Prefix (Secondary Signal)**

When location data does not uniquely identify a campus, the incident number prefix is used as a secondary indicator.

Example prefixes include:

*   `26UP` → University Park
*   `26AB` → Abington
*   `26YK` → York

Incident prefixes are not treated as authoritative on their own but are used to infer campus only when no contradicting location information exists.

***

### 3. Other Penn State Campuses (Out of Scope but Valid)

Incidents belonging to Penn State campuses not central to the primary analysis (e.g., York, Shenango, Scranton, DuBois, Wilkes‑Barre) are retained and assigned to their respective campus directories.

These datasets are preserved to:

*   Avoid data loss
*   Prevent contamination of Abington or University Park analyses
*   Allow later comparative or expansion studies

---

### **4. Ambiguous Geographic Locations (No Assumptions)**

Some location descriptions are geographically ambiguous and cannot be reliably associated with a Penn State campus without additional context.

Examples include:

*   `Pennsylvania Ave`
*   `University Dr`
*   `1st Ave`
*   `Depot St`

These locations may exist in multiple municipalities or near multiple institutions. **No attempt is made to guess campus assignment** for these records.

Such incidents are explicitly placed in an `ambiguous_locations` directory and excluded from campus-specific analysis.

---

### **5. Off-Campus and Unknown Locations**

Incidents explicitly marked as:

*   `OFF CAMPUS`
*   `UNKNOWN LOCATION`
*   `UNKNOWN ON CAMPUS PROPERTY`

are assigned to an `off_or_unknown` category. These records are preserved but not included in campus-level datasets, as their attribution cannot be verified.

---

## **Relocation and Auditability**

To maintain transparency, the pipeline records all cases where the initial incident-number-based assignment differs from the final campus assignment.

These records are saved to a `relocation_audit.csv` file, which includes:

*   Incident number
*   Original inferred campus
*   Final assigned campus
*   Location text

This audit log ensures that all reassignment decisions are traceable and reviewable.

---

## **Output Organization**

Final outputs are organized as follows:

*   `data/raw/`: Original unmodified data
*   `data/processed/`: Cleaned and normalized datasets
*   `data/campuses/`: Campus-specific incident datasets, including:
    *   Individual Penn State campuses
    *   `ambiguous_locations`
    *   `off_or_unknown`

This structure separates canonical datasets from derived analytical outputs and supports downstream exploration without recomputation.

---

## **Design Principles and Limitations**

### **Design Principles**

*   **Conservatism**: It is preferable to leave an incident unassigned than to misassign it.
*   **Transparency**: Every inference step is logged or reproducible.
*   **Reproducibility**: The entire pipeline can be re-run from raw data.

### **Limitations**

*   Location parsing relies on free-text descriptions, which may be inconsistent.
*   Some campus facilities share generic names (e.g., “Student Union”), requiring careful curation.
*   The pipeline does not perform geocoding; future work could incorporate address-based validation.

---

## **Summary**

This methodology ensures that Penn State crime log incidents are organized accurately, defensibly, and transparently. By combining authoritative location matching with conservative fallback logic and explicit ambiguity handling, the pipeline prioritizes data integrity while retaining flexibility for future refinement.



