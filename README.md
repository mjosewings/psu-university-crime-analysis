# 📊 PSU Campus Crime Analysis

## 📌 Project Overview
This project explores publicly available daily crime logs from Penn State University campuses, with an initial focus on **Penn State Abington**. Using structured data collection, relational database design, and SQL-based analysis, the project aims to identify patterns, trends, and insights related to campus safety and reported incidents.

All data used in this project is sourced from **official Penn State University Police public records** and is aggregated to protect individual privacy.  
**No personally identifiable information (PII)** is stored or analyzed.

---

## 🎯 Project Objectives
1. Design and implement a normalized relational database for campus crime data  
2. Collect and clean publicly available daily crime log data  
3. Analyze incident frequency, type, and location trends across campuses  
4. Compare crime patterns between Penn State campuses  
5. Support data-driven discussions on improving campus safety  
6. Export structured datasets for visualization and reporting  

---

## 🧠 Data Source
**Penn State University Police – Daily Crime Log**  
Publicly available data collected from:  
https://www.police.psu.edu/daily-crime-log  

All data used in this project is publicly accessible and does not include private or sensitive personal information.

---

## 🗂️ Database Design
The database is normalized and designed using a **relational model**. Core tables include:

- `campuses` – Campus metadata  
- `locations` – Incident locations linked to campuses  
- `incidents` – Individual crime log entries  
- `offenses` – Standardized offense categories  

The schema is designed to support:
- Multi-campus analysis  
- Efficient querying  
- Clean exports for downstream visualization  

---

## 🛠️ Tools & Technologies
- **Database:** MySQL  
- **Version Control:** Git & GitHub  
- **Statistical Analysis:** R, Python  
- **Data Visualization:** `matplotlib`, `ggplot2`  

---

## 📦 Repository Structure
```text
psu-university-crime-analysis/
│
├── data/
│   ├── raw/                    # Original, unmodified data
│   │   ├── psu_crime_log_records.json
│   │   └── psu_crime_log.db
│   │
│   ├── processed/              # Cleaned & standardized datasets
│   │   ├── incidents.csv
│   │   ├── campuses.csv
│   │   ├── offense_types.csv
│   │   ├── incident_offenses.csv
│   │   └── relocation_audit.csv
│   │
│   └── campuses/               # Campus-split outputs
│       ├── University_Park/
│       ├── Abington/
│       ├── York/
│       ├── Shenango/
│       ├── Scranton/
│       ├── Hershey/
│       ├── ambiguous_locations/
│       └── off_or_unknown/
│
├── src/
│   ├── scraping/
│   │   └── psu_crime_scraper.py
│   │
│   ├── preprocessing/
│   │   ├── split_incidents_by_campus.py
│   │   ├── relocate_incidents.py
│   │   └── run_cleanup.py
│   │
│   └── sql/
│       └── clean_campus_codes.sql
│
├── docs/
│   ├── methodology.md
│   ├── data_dictionary.md
│   └── campus_mapping.md
│
├── README.md
├── LICENSE
├── .gitignore
└── requirements.txt
``