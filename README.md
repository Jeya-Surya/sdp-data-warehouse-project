# 📊 DataWarehouse Data Pipeline using Spark Declarative Pipelines (SDP)

## 📌 Project Overview

This project implements an **end-to-end Lakehouse data pipeline** using the **Bronze → Silver → Gold architecture**.  
Raw data from multiple **CRM and ERP sources** is ingested, cleaned, standardized, and transformed into analytics-ready **dimension and fact tables**.

The pipeline is built using **Spark Declarative Pipelines (SDP)** in Databricks and is implemented in **both PySpark and SQL**, demonstrating semantic equivalence and strong data engineering fundamentals.

---

## 🟤 Bronze Layer – Raw Ingestion

**Purpose:**  
Capture raw data exactly as received from source systems.

**Key Characteristics:**
- Ingests CSV files from CRM and ERP systems
- No business logic applied
- Schema inferred at ingestion
- One table per source

**Tables:**
- `bronze_crm_customers`
- `bronze_crm_products`
- `bronze_crm_sales`
- `bronze_erp_customer_details`
- `bronze_erp_customer_location`
- `bronze_erp_product_details`

---

## 🥈 Silver Layer – Data Standardization

**Purpose:**  
Clean, validate, and standardize data while preserving business correctness.

**Key Transformations:**
- Deterministic deduplication using window functions
- Data quality corrections (invalid dates, incorrect sales values)
- Standardized codes (gender, marital status, country, product line)
- Type casting and normalization
- Harmonization between CRM and ERP sources

**Tables:**
- `silver_crm_customers`
- `silver_crm_products`
- `silver_crm_sales`
- `silver_erp_customer_details`
- `silver_erp_customer_location`

---

## 🥇 Gold Layer – Dimensional Model

**Purpose:**  
Provide analytics-ready data optimized for reporting and BI tools.

### Dimensions
- `gold_dim_customers`
- `gold_dim_products`

### Fact Table
- `gold_fact_sales`

**Modeling Approach:**
- Star schema
- Surrogate keys generated using window functions
- Business keys preserved in dimensions
- Clear grain and join paths
- CRM treated as primary source, ERP as enrichment

---

## 🛠️ Technology Stack

- Databricks
- Spark Declarative Pipelines (SDP)
- PySpark
- Spark SQL
- Delta Lake
- GitHub (version control)

---

## ✨ Key Highlights

- End-to-end **Bronze → Silver → Gold** pipeline
- Deterministic transformations (no random deduplication)
- Business-rule driven data corrections
- Proper dimensional modeling (fact & dimensions)
- Both **PySpark and SQL implementations**
- Production-style lakehouse design





