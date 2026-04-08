
# **Data Dictionary**

## **Overview**

This document describes the structure and semantics of all primary datasets produced by the PSU Crime Log data pipeline. The goal of this data dictionary is to make each dataset interpretable without requiring direct inspection of the preprocessing code.

All datasets are derived from the Penn State Daily Crime Log and have undergone normalization, cleaning, and campus validation as described in `methodology.md`.

---

## **`incidents.csv`**

### **Description**

Contains one record per reported crime log incident. This is the primary fact table for the project.

### **Fields**

*   **`id`**  
    Unique internal identifier for the incident record.

*   **`incident_number`**  
    Official Penn State Police incident identifier.  
    Often includes a campus-specific prefix (e.g., `26UP`, `26AB`).

*   **`campus_id`**  
    Numeric campus identifier as provided in the original source data.  
    *Note: This field is not always reliable for campus assignment.*

*   **`reported_datetime`**  
    Date and time when the incident was reported to authorities.

*   **`occurred_start`**  
    Start date and time when the incident is reported to have occurred.

*   **`occurred_end`**  
    End date and time of the incident (if applicable).  
    May be null for instantaneous or unknown-duration incidents.

*   **`nature_of_incident`**  
    Free-text description summarizing the reported incident.

*   **`location`**  
    Free-text location description provided in the crime log.

*   **`created_at`**  
    Timestamp indicating when the record was created in the dataset.

*   **`final_campus`**  
    Canonical campus assignment derived during preprocessing.  
    Possible values include:
    *   Named Penn State campuses (e.g., `University_Park`, `Abington`, `York`)
    *   `ambiguous_locations`
    *   `off_or_unknown`

---

## **`campuses.csv`**

### **Description**

Lookup table mapping campus identifiers to canonical campus names.

### **Fields**

*   **`campus_id`**  
    Numeric identifier associated with a Penn State campus in the source data.

*   **`campus_code`**  
    Short code representing the campus (e.g., `UP`, `AB`, `YK`).

*   **`campus_name`**  
    Human-readable campus name (e.g., `University Park`, `Abington`).

---

## `offense_types.csv`

### **Description**

Lookup table defining standardized offense categories.

### **Fields**

*   **`offense_id`**  
    Unique identifier for an offense type.

*   **`offense_code`**  
    Short code representing the offense category.

*   **`offense_description`**  
    Textual description of the offense type.

---

## `incident_offenses.csv`

### **Description**

Join table representing the many-to-many relationship between incidents and offenses.  
An incident may involve multiple offense types.

### **Fields**

*   **`incident_id`**  
    Foreign key referencing `incidents.id`.

*   **`offense_id`**  
    Foreign key referencing `offense_types.offense_id`.

---

## `relocation_audit.csv`

### **Description**

Audit log capturing incidents whose campus assignment changed during preprocessing.

This dataset exists to ensure transparency and reproducibility of campus reassignment decisions.

### **Fields**

*   **`incident_number`**  
    Original incident identifier.

*   **`prefix_campus`**  
    Campus inferred from the incident number prefix.

*   **`location_campus`**  
    Campus inferred from location text (if any).

*   **`final_campus`**  
    Campus ultimately assigned after resolution logic.

*   **`location`**  
    Original free-text location field for reference and review.

---

## **Campus-Specific Datasets (`data/campuses/`)**

### **Description**

Each subdirectory under `data/campuses/` contains an `incidents.csv` file restricted to a single campus or classification category.

Examples:

*   `University_Park/incidents.csv`
*   `Abington/incidents.csv`
*   `York/incidents.csv`
*   `ambiguous_locations/incidents.csv`
*   `off_or_unknown/incidents.csv`

### **Notes**

*   Campus-specific datasets preserve all columns from `incidents.csv`.
*   No additional transformations occur at this stage; records are filtered exclusively by `final_campus`.

---

## **Data Quality Notes**

*   Location fields are free-text and may vary in specificity and formatting.
*   Campus assignment prioritizes known Penn State buildings and facilities.
*   Incidents with ambiguous or unverifiable locations are intentionally excluded from campus directories to prevent misclassification.
*   All assignments and relocations are reproducible via the preprocessing scripts.

---

## **Summary**

This data dictionary provides a complete reference for interpreting all datasets produced by the PSU Crime Log pipeline. When used alongside `methodology.md`, it ensures that the data can be confidently analyzed, audited, and extended without ambiguity.
