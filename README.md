#  RetailOps - Orchestrated Retail Data Lakehouse

**Project Status: Completed**

## 1. Overview

RetailOps is an end-to-end, automated data lakehouse platform for retail analytics. This project ingests both batch (CSV) and real-time (Kafka) retail sales data, processes it through a multi-layered Medallion Architecture, and provides actionable insights via Databricks SQL dashboards.

The entire pipeline is built on Databricks and is fully automated using Databricks Jobs for orchestration.

## 2. System Architecture

The project follows a **Medallion Architecture** to ensure data quality and progressive refinement.



1.  **Ingestion:**
    * **Batch:** A notebook (`batch_ingest.py`) reads CSV files that are dropped into a landing zone.
    * **Streaming:** A separate notebook (`stream_ingest.py`) connects to a Kafka topic to ingest real-time sales events.
    * Both ingestion scripts are robust, quarantining bad or malformed data into a separate table (`raw_quarantine`) and only writing good data to the Bronze layer.

2.  **Bronze Layer (Raw Data):**
    * **Table:** `retailops.default.raw_retail`
    * **Purpose:** This table holds the raw, unprocessed data from both batch and streaming sources. It serves as the "single source of truth."

3.  **Silver Layer (Cleaned Data):**
    * **Table:** `retailops.default.clean_retail`
    * **Purpose:** A Delta Live Tables (DLT) pipeline reads from Bronze, applies data quality rules (e.g., non-null checks, deduplication), and saves the clean, validated, and standardized data here.

4.  **Gold Layer (Aggregated Data):**
    * **Table:** `retailops.default.retail_metrics`
    * **Purpose:** The DLT pipeline aggregates the clean Silver data into business-ready metrics (e.g., total daily sales, units sold by store) for fast and easy querying by analytics dashboards.

5.  **Orchestration & Analytics:**
    * **Databricks Jobs:** A single job orchestrates the entire pipeline:
        1.  Runs the batch and streaming ingestion tasks in parallel.
        2.  Once both are complete, it triggers the DLT pipeline (`Run_Retail_Pipeline`) to process the new data.
    * **Databricks SQL:** SQL queries and dashboards connect directly to the Gold table for visualization.

## 3. Technology Stack

This project uses the following technologies as defined in the project documents:

* **Cloud Platform:** Databricks
* **Data Ingestion:**
    * **Batch:** PySpark (for CSVs)
    * **Streaming:** Apache Kafka (Confluent Cloud)
* **ETL & Pipelines:** Delta Live Tables (DLT)
* **Data Storage:** Delta Lake
* **Data Quality:** DLT Expectations (`@dlt.expect_or_quarantine`)
* **Orchestration:** Databricks Jobs
* **Analytics:** Databricks SQL

## 4. Project Folder Structure
