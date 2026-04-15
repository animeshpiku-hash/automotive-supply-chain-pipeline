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

## Layers
- **Bronze:** Raw data ingestion (CSV/JSON from suppliers)
- **Silver:** Data quality checks, cleansing, standardization
- **Gold:** Star schema for analytics (fact_deliveries, dim_supplier, dim_product, dim_date)

## Setup Instructions
Coming soon - Week 1 in progress!

## Project Structure
.
├── notebooks/          # Databricks notebooks
├── data/              # Sample data files
├── docs/              # Documentation
└── README.md          # This file

## Progress Log
- [x] Day 1: Azure setup (ADLS Gen2, Resource Group)
- [x] Day 2: Databricks workspace + GitHub setup
- [x] Day 3: Bronze layer implementation ✨
  - Connected ADLS to Databricks (ABFS method)
  - Loaded supplier delivery data (10 records)
  - Created bronze Delta table with audit columns
- [ ] Day 4: Silver layer implementation

## Author
Animesh Mishra - Data Engineer in Training

## License
MIT License