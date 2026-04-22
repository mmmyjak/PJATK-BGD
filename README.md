# Airline Delays - ELT Pipeline (Medallion Architecture)

A project implementing an automated Extract, Load, Transform (ELT) data pipeline for historical US commercial flight data. The solution is based on the Medallion Architecture (Bronze, Silver, Gold) utilizing **Apache Spark (PySpark)** running in a **Docker** container and a **PostgreSQL** data warehouse.

## 📂 Repository Structure

* **`docs/`** - Analytical and design documentation (Deliverables):
  * [`problem-statement.md`](docs/problem-statement.md) - The main business and analytical goal of the project.
  * [`architecture_hla.md`](docs/architecture_hla.md) - High-level architecture of the solution (Mermaid).
  * [`architecture_lld.md`](docs/architecture_lld.md) - Low-level design of the solution (Mermaid).
  * [`erd.md`](docs/erd.md) - Entity-Relationship Diagram (ERD) of the PostgreSQL database (Mermaid).
  * [`data_quality_risks.md`](docs/data_quality_risks.md) - Identification of 3 main data quality risks (Ingestion, Transformation, Scale).
* **`db/`** - Database scripts:
  * `001_init_schemas.sql` - Script initializing schemas (bronze, silver, gold) and log tables for incremental loading.
* **`src/`** - Main data pipeline code (PySpark):
  * `bronze_ingest.py` - Ingests raw CSV files and appends them to the database.
  * `silver_clean.py` - Data cleansing, type casting, and NULL handling (incremental load).
  * `gold_aggregate.py` - Business aggregations (reference only; Gold layer is now handled by dbt).
  * `dags/medallion_pipeline_dag.py` - Airflow DAG defining the full pipeline as a single entry point.
  * `dags/medallion_pipeline_kafka_dag.py` - Airflow DAG triggered automatically from Kafka events.
  * `kafka_source_producer.py` - Watches source files and publishes file events to Kafka.
  * `kafka_airflow_trigger.py` - Consumes Kafka events and triggers the Kafka Airflow DAG.
* **`dbt/`** - Gold layer transformation declarations (dbt-postgres):
  * `dbt_project.yml` - Project config; artifacts written to `data/dbt/` (gitignored).
  * `profiles.yml` - Connection profile reading from environment variables.
  * `models/sources.yml` - Declares `silver.flights` as the dbt source.
  * `models/gold/monthly_carrier_performance.sql` - Incremental model: carrier KPIs by month.
  * `models/gold/yearly_route_reliability.sql` - Incremental model: route reliability by year.
  * `models/gold/schema.yml` - Data quality tests (`not_null`) for Gold models.

## 🚀 How to Run the Project

The project is fully containerized to avoid local environment issues (e.g., Java/Hadoop configurations on Windows).

1. Clone the repository and ensure your raw CSV files are placed in the `data/` directory (this folder is ignored via `.gitignore`).
2. Start the infrastructure (PostgreSQL + Spark Workspace) in the background:
   ```bash
   docker-compose up -d --build
  ```

Optional Kafka auto-trigger mode (in addition to manual Airflow triggering):

```bash
docker compose up -d kafka kafka-source-producer kafka-airflow-trigger
```

This keeps your existing manual DAG (`medallion_airline_pipeline`) and adds automatic triggering through `medallion_airline_pipeline_kafka`.