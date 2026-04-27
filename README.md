# Automotive Supply Chain Analytics Pipeline

## Project Overview
End-to-end data engineering pipeline implementing medallion architecture (bronze/silver/gold layers) for automotive supply chain analytics.

## Architecture
Data Sources → Azure Data Lake (Bronze) → Databricks (Silver) → Databricks (Gold) → Analytics/BI
## Tech Stack
- **Cloud:** Azure
- **Storage:** Azure Data Lake Storage Gen2
- **Compute:** Azure Databricks (Apache Spark)
- **Language:** Python (PySpark, pandas)
- **Data Format:** Delta Lake
- **Version Control:** Git/GitHub

##  Data Layers

###  Bronze Layer
- Raw ingestion from supplier files (CSV/JSON)
- Stored in Delta format
- Includes audit columns (ingestion timestamp, source)

###  Silver Layer *(In Progress)*
- Data cleansing (null handling, duplicates removal)
- Standardization of formats
- Data quality validation

###  Gold Layer *(Planned)*
- Star schema design:
  - `fact_deliveries`
  - `dim_supplier`
  - `dim_product`
  - `dim_date`
- Optimized for analytics and reporting


## Setup Instructions
Coming soon - Week 1 in progress!

## Project Structure
├── notebooks/          # Databricks notebooks
├── data/              # Sample data files
├── docs/              # Documentation
└── README.md          # This file
## Progress Log

- [x] Day 1: Azure setup (ADLS Gen2, Resource Group)  
- [x] Day 2: Databricks workspace + GitHub integration  
- [x] Day 3: Bronze layer implementation ✨  
  - Mounted ADLS to Databricks  
  - Created Bronze Delta table  
  - Loaded supplier delivery dataset (10 records)  
- [ ] Day 4: Silver layer implementation  

---

##  Key Learnings
- Implemented medallion architecture using Azure + Databricks  
- Built data ingestion pipeline using Delta Lake  
- Practiced Git-based version control for data projects  

---

## Author
**Animesh Mishra**  
Aspiring Data Engineer | Building end-to-end data pipelines

---

## 📜 License
MIT License
