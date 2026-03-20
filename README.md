# Enterprise Data Warehouse: Medallion Architecture

## 📌 Project Overview
This project demonstrates the end-to-end engineering of a robust Enterprise Data Warehouse utilizing Microsoft SQL Server. The primary objective was to extract, transform, and load (ETL) disparate datasets from fragmented CRM and ERP systems, integrating them into a governed, business-ready Star Schema. 

The pipeline strictly adheres to the **Medallion Architecture** (Bronze, Silver, Gold), ensuring data traceability, quality, and analytical efficiency.

![High Level Architecture](assets/HighLevelArchitecture1.png)

## 🛠️ Technology Stack
* **Database Engine:** Microsoft SQL Server
* **Language:** T-SQL (Transact-SQL)
* **Architecture Pattern:** Medallion Architecture, Dimensional Modeling (Star Schema)
* **Core Concepts:** Data Integration, ETL Automation, Defensive Programming, Master Data Management (MDM), Surrogate Keys, SCD Type 2.

---

## 🏗️ Architecture & Data Flow

![Data Flow Diagram](assets/DataFlowDiagram.png)

### 🥉 Bronze Layer (Raw Ingestion)
The Bronze layer acts as the initial landing zone for raw data extracts. 
* **Execution:** Automated via `BULK INSERT` statements.
* **Data Types:** Ingested as `NVARCHAR` to prevent upstream data type anomalies from crashing the ingestion process.

### 🥈 Silver Layer (Cleansing & Integration)
The Silver layer is the transformation hub. Here, data is standardized, cleansed, and integrated.
* **Data Integration:** Combined CRM and ERP data using precise `JOIN`s and string manipulations (`SUBSTRING`, `TRIM`).
* **Data Quality:** Handled `NULL` values, standardized date formats, and resolved negative or invalid financial entries.
* **Normalization:** Applied strict `CASE` statements to standardize geospatial data and categorize business logic.

![Integration Model](assets/IntegrationModel.png)

### 🥇 Gold Layer (Business & Reporting)
The Gold layer serves as the Semantic Layer, optimized entirely for Business Intelligence (BI) and ad-hoc reporting.
* **Virtualization:** Constructed entirely using SQL `VIEW`s to optimize storage overhead while ensuring zero-latency access to the most recent Silver-layer transformations.
* **Star Schema:** Fact and Dimension tables architected for intuitive BI querying.

![Data Model](assets/DataModel.png)

---

## 🚀 Enterprise Data Warehouse Engineering (Core Achievements)
This project required solving complex data engineering challenges, resulting in the following technical implementations:

* **Data Architecture:** Designed and deployed a multi-layer Data Warehouse (Medallion Architecture) in SQL Server to integrate disparate CRM and ERP source systems.
* **ETL Pipeline Automation:** Engineered robust, idempotent T-SQL Stored Procedures utilizing `BULK INSERT` to automate high-volume data ingestion, complete with dynamic execution logging and `TRY...CATCH` error handling.
* **Data Quality & Governance:** Implemented comprehensive Silver-layer validation scripts to enforce referential integrity, perform deduplication using Window Functions (`ROW_NUMBER()`), and standardize categorical variables.
* **Defensive Pipeline Engineering:** Utilized `TRY_CAST` methodologies to ensure pipeline resilience against upstream data anomalies and prevent catastrophic ETL failures.
* **Master Data Management (MDM):** Engineered cross-system survivorship rules and geospatial transformations to resolve data conflicts between CRM and ERP systems, establishing a Single Source of Truth.
* **Dynamic Fact Calculations:** Engineered self-healing arithmetic validations (`NULLIF`, `ABS`) to automatically recalculate missing or anomalous financial metrics (Sales = Price * Quantity) during ingestion.
* **Dimensional Modeling (Star Schema):** Architected a virtualized Gold layer utilizing SQL Views; generated robust Surrogate Keys to insulate downstream reporting from source-system volatility.
* **Semantic Layer Design:** Transformed cryptic source schemas into standardized, user-friendly business terms, accelerating downstream BI dashboard development and minimizing ad-hoc data requests.
* **Referential Integrity Testing:** Developed automated Foreign Key integrity checks to validate complete Dimensional linkage prior to exposing the Gold layer to Business Stakeholders.

## 📂 Repository Structure
* `src/bronze/`: DDL and `BULK INSERT` scripts for raw data.
* `src/silver/`: Cleansing transformations and Data Quality Stored Procedures.
* `src/gold/`: View definitions for the Star Schema (Dimensions and Facts).
* `tests/`: SQL scripts for Referential Integrity and Foreign Key testing.
* `assets/`: Architectural diagrams and schema maps.
