# Project Architecture & Data Model

This document outlines the high-level architecture of the ELT pipeline and the Entity-Relationship Diagram (ERD) of the Medallion Architecture implemented in PostgreSQL.

## 1. High-Level Pipeline Architecture

The pipeline extracts raw CSV data (~10 GB), processes it using Apache Spark (PySpark) within an isolated Docker container, and loads it incrementally into a PostgreSQL data warehouse. The entire workflow is orchestrated by Apache Airflow.

```mermaid
graph TD
    subgraph Orchestration Layer
        Airflow[Apache Airflow<br/>DAG: medallion_airline_pipeline]
    end

    subgraph Local Environment
        A[Kaggle CSV Files<br/>Airline Delays]
    end

    subgraph Execution Environment
        B((PySpark Workspace<br/>Python 3.11 + Java 21))
    end

    subgraph PostgreSQL Database
        C[(schema: bronze<br/>Raw Data)]
        D[(schema: silver<br/>Cleaned Data)]
        E[(schema: gold<br/>Aggregated Data)]
    end

    A -->|Volume Mount| B

    %% Control Flow (Orchestration)
    Airflow -.->|1. docker exec bronze_ingest.py| B
    Airflow -.->|2. docker exec silver_clean.py| B
    Airflow -.->|3. docker exec gold_aggregate.py| B

    %% Data Flow (ELT Process)
    B ==>|Ingest & Append| C
    C ==>|PySpark Reads & Cleans| D
    D ==>|PySpark Reads & Aggregates| E