# 📊 PSU Campus Crime Analysis

## 📌 Project Overview
This project explores publicly available daily crime logs from Penn State University campuses, with an initial focus on Penn State Abington. Using structured data collection, relational database design, and SQL-based analysis, the project aims to identify patterns, trends, and insights related to campus safety and reported incidents.

All data used in this project is sourced from official Penn State University Police public records and is aggregated to protect individual privacy. No personally identifiable information (PII) is stored or analyzed.

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
- **Penn State University Police - Daily Crime Log**
  Publicly avaliable data collected from: https://www.police.psu.edu/daily-crime-log

All data used in this project is publicly accessible and does not include private or sensitive personal information.

--- 
## 🗂️ Database Design
The database is normalized and designed using a relational model. Core tables include:

- `campuses` - Campus metadata
- `locations` - Incident locations linked to campuses
- `incidents` - Individual crime log entries
- `offenses` - Stamdardized offense categories

The schema is designed to support multi-campus analysis, efficient querying, and clean exports for downstream visualization

---
## 🛠️ Tools & Technologies
- Database: MySQL
- Verison Control: Git & GitHub
- Statistical Analysis: R and Python
- Data Visualization: matplotlib, ggplot2

---
## 📦 Repository Structure
```psu-campus-crime-analysis/
├── sql/
│   ├── schema/
│   │   └── create_database.sql        # Campus metadata
│   │  
│   ├── inserts/
│   │   ├── campuses.sql             # Campus reference data
│   │   ├── locations.sql            # Location reference data
│   │   └── offenses.sql             # Offense reference data
│   ├── queries/
│   │   ├── exploratory.sql          # Initial data exploration
│   │   ├── trends.sql               # Temporal trend analysis
│   │   ├── campus_comparisons.sql   # Cross-campus comparisons
│   │   └── exports.sql              # CSV export queries
│
├── data/
│   ├── raw/
│   │   ├── abington_raw.csv
│   │   └── university_park_raw.csv
│   ├── cleaned/
│   │   ├── abington_cleaned.csv
│   │   └── university_park_cleaned.csv
│   ├── merged/
│   │   └── incidents_analysis.csv  # Denormalized analysis-ready dataset
│
├── analysis/
│   ├── notebooks/
│   │   └── crime_trends.ipynb
│   └── scripts/
│       └── summary_statistics.py
│
├── figures/
│   ├── incidents_by_month.png
│   ├── offenses_by_type.png
│   └── campus_comparison.png
│
├── docs/
│   ├── proposal.md                  # Project proposal
│   ├── methodology.md               # Data collection & ethics
│   ├── data_dictionary.md           # Column definitions
│   └── references.md                # Sources & citations
│
├── README.md
└── LICENSE 
```

---
## 📚 Academic Context
This project is developed as part of a data mining and database-focused academic course at Penn State Abington. 

It integrates:
- Relational database design
- Data ethics and privacy
- Applied SQL querying
- Real-world public data analysis

The project is suitable for expansion into research, campus safety studies, or advanced analytics coursework.
