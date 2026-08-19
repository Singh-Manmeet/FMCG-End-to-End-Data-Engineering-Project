# 🛒 End-to-End FMCG Data Engineering Project

An end-to-end **Data Engineering project** built using **Databricks, Apache Spark, Python, SQL, and Amazon S3**. The project simulates the integration of data from a parent FMCG company, **Atlon**, and an acquired startup, **Sports Bar**, into a unified **Lakehouse platform**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, clean, transform, and aggregate data for business analytics.

---

## 📌 Project Overview

### Business Scenario

**Atlon**, an FMCG parent company, acquired a startup called **Sports Bar**. Both organizations maintained their data independently with differences in:

- Data formats
- Schemas
- Data quality
- Naming conventions
- Missing values
- Product and customer information

The objective is to build a **scalable data platform** that consolidates data from both organizations and provides a single source of truth for analytics.

### 🎯 Project Goal

Build an automated **ETL/ELT pipeline** that:

1. Ingests raw data from **Amazon S3**
2. Stores raw data in the **Bronze layer**
3. Cleans and standardizes data in the **Silver layer**
4. Creates business-ready datasets in the **Gold layer**
5. Combines data from **Atlon and Sports Bar**
6. Enables analytics through **Databricks SQL and Genie**
7. Automates the pipeline using **Databricks Jobs**


## 🥉 Bronze Layer — Raw Data

The **Bronze layer** acts as the initial landing layer for source data.

### Responsibilities

- Ingest raw files from **Amazon S3**
- Preserve the original source data
- Handle initial schema inference
- Add ingestion/audit information
- Support incremental ingestion
- Maintain source-level traceability

The Bronze layer is intentionally kept close to the original source format so that data can be reprocessed if required.

---

## 🥈 Silver Layer — Cleaned & Standardized Data

The Silver layer contains **cleaned and standardized datasets**.

### Transformations

- Handle **NULL and missing values**
- Remove duplicate records
- Correct inconsistent data
- Standardize column names
- Resolve schema differences between Atlon and Sports Bar
- Apply data type conversions
- Standardize business fields
- Perform data quality validations

The objective is to create **reliable and consistent datasets** for downstream processing.

---

## 🥇 Gold Layer — Business-Ready Data

The Gold layer contains **aggregated and analytics-ready datasets**.

### Responsibilities

- Combine data from **Atlon and Sports Bar**
- Create business-level aggregations
- Generate denormalized reporting tables
- Calculate revenue and sales metrics
- Analyze product performance
- Analyze channel performance
- Prepare datasets for BI and business users

The Gold layer acts as the primary **analytics consumption layer**.

---

## ⚙️ Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | Data processing and pipeline development |
| **SQL** | Data transformation and analytics |
| **Apache Spark** | Distributed data processing |
| **Databricks** | Lakehouse platform and pipeline execution |
| **Amazon S3** | Cloud object storage / data lake |
| **Delta Lake** | Reliable storage and table management |
| **Medallion Architecture** | Bronze-Silver-Gold data organization |
| **Databricks Jobs** | Pipeline orchestration and scheduling |
| **Databricks Genie** | Natural language analytics |
| **Databricks SQL** | Querying and analytics |

---

## 🔁 Incremental Data Processing

The pipeline is designed to support **incremental processing** instead of processing the complete dataset every day.

The general flow is:

```text
New Data Arrives
       │
       ▼
Amazon S3
       │
       ▼
Identify New Records
       │
       ▼
Bronze
       │
       ▼
Silver Transformations
       │
       ▼
Gold Aggregations
```

This approach helps reduce unnecessary processing and makes the pipeline more scalable.

---

## 🧹 Data Quality

Data quality checks are performed during the transformation process.

Examples include:

- **NULL validation**
- Duplicate detection
- Data type validation
- Schema validation
- Invalid value checks
- Standardization of business fields
- Referential/data consistency checks

These validations help ensure that only **trusted data** reaches the Gold layer.

---

## 📊 Analytics

The Gold layer provides datasets for business analytics such as:

### Revenue Analysis

- Total revenue
- Revenue by product
- Revenue by region
- Revenue by channel

### Product Analysis

- Product performance
- Top-selling products
- Product trends
- Category-level analysis

### Channel Analysis

- Sales by channel
- Channel performance
- Revenue contribution
- Customer purchasing trends

---

## 🤖 Databricks Genie

**Databricks Genie** is used to provide a natural-language interface over the analytics datasets.

Instead of writing SQL manually, business users can ask questions such as:

```text
What was the total revenue last month?

Which products generated the highest revenue?

Which sales channel performed best?

What are the top 10 products by revenue?
```

Genie translates natural-language questions into queries against the available analytical data.



## 📈 Expected Outcome

After successful execution, the project provides:

- A centralized **Lakehouse platform**
- Consolidated data from **Atlon and Sports Bar**
- Clean and standardized datasets
- Business-ready Gold tables
- Incremental data processing
- Automated pipeline orchestration
- Analytics-ready datasets
- Natural-language analytics through **Databricks Genie**

---

## 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- **ETL / ELT**
- **Medallion Architecture**
- **Data Lakehouse**
- **Apache Spark**
- **PySpark**
- **SQL**
- **Delta Lake**
- **Incremental Data Processing**
- **Data Quality**
- **Schema Evolution / Standardization**
- **Data Transformation**
- **Data Aggregation**
- **Pipeline Orchestration**
- **Cloud Storage**
- **Analytics Engineering**
- **Databricks Genie**

---

## 👨‍💻 Project Purpose

This project was created as a hands-on demonstration of designing and implementing an **end-to-end modern data engineering pipeline** using cloud storage, distributed processing, Lakehouse architecture, and business analytics.

It focuses on the complete journey of data:

**Raw Data → Ingestion → Cleaning → Transformation → Aggregation → Analytics**