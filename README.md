# ETL_With_Apache_Spark
# ETL with Apache Spark on Databricks – GizmoBox E-commerce Data Engineering

## Overview

This project demonstrates an end-to-end data engineering workflow using **Apache Spark** on **Databricks** for an imaginary e-commerce platform, **GizmoBox**. The ETL (Extract, Transform, Load) pipeline ingests, processes, and integrates multiple data sources to provide a unified analytics-ready dataset.

## Data Sources

The following datasets are utilized in this project:

- **Customers**: JSON file containing customer profiles.
- **Orders**: Complex JSON file representing order transactions, including nested structures.
- **Addresses**: Tab-separated CSV file with customer address information.
- **Payments**: CSV file loaded into an external table.
- **Refunds**: Table sourced from Azure SQL Database via JDBC.

## Technologies Used

- **Apache Spark**: Distributed data processing engine.
- **Databricks**: Cloud-based analytics platform for big data and machine learning.
- **Azure SQL Database**: For storing and accessing refunds data.
- **JDBC**: For connecting Databricks to Azure SQL.
- **Python / PySpark**: For coding the ETL pipeline.

## ETL Workflow

1. **Extraction**
   - Read customer and order data from JSON files.
   - Load address data from a tab-separated CSV file.
   - Create an external table on the payment CSV data.
   - Connect to Azure SQL using JDBC to access the refunds table.

2. **Transformation**
   - Parse and flatten complex/nested JSON structures (orders).
   - Standardize address formats.
   - Join datasets on relevant keys (e.g., customer ID).
   - Cleanse and validate payment and refund records.

3. **Loading**
   - Store the integrated dataset in a format suitable for further analytics (e.g., Parquet, Delta Lake).
   - Optionally, write back to external systems or the data warehouse.

## Folder Structure
