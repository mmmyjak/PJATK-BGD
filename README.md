# Airline Delays - ELT Pipeline (Medallion Architecture)

A project implementing an automated Extract, Load, Transform (ELT) data pipeline for historical US commercial flight data. The solution is based on the Medallion Architecture (Bronze, Silver, Gold) utilizing **Apache Spark (PySpark)** running in a **Docker** container and a **PostgreSQL** data warehouse.

## 📂 Repository Structure

* **`docs/`** - Analytical and design documentation (Deliverables):
  * [`problem-statement.md`](docs/problem-statement.md) - The main business and analytical goal of the project.
  * [`architecture.md`](docs/architecture.md) - High-level architecture diagram of the solution (Mermaid).
  * [`erd.md`](docs/erd.md) - Entity-Relationship Diagram (ERD) of the PostgreSQL database (Mermaid).
  * [`data_quality_risks.md`](docs/data_quality_risks.md) - Identification of 3 main data quality risks (Ingestion, Transformation, Scale).
* **`db/`** - Database scripts:
  * `001_init_schemas.sql` - Script initializing schemas (bronze, silver, gold) and log tables for incremental loading.
* **`src/`** - Main data pipeline code (PySpark):
  * `bronze_ingest.py` - Ingests raw CSV files and appends them to the database.
  * `silver_clean.py` - Data cleansing, type casting, and NULL handling (incremental load).
  * `gold_aggregate.py` - Business aggregations (JOIN/GroupBy) for airline and route performance (incremental load).

## 🚀 How to Run the Project

The project is fully containerized to avoid local environment issues (e.g., Java/Hadoop configurations on Windows).

1. Clone the repository and ensure your raw CSV files are placed in the `data/` directory (this folder is ignored via `.gitignore`).
2. Start the infrastructure (PostgreSQL + Spark Workspace) in the background:
   ```bash
   docker-compose up -d --build