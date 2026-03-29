# Project Architecture & Data Model

This document outlines the high-level architecture of the ELT pipeline and the Entity-Relationship Diagram (ERD) of the Medallion Architecture implemented in PostgreSQL.

## 1. High-Level Pipeline Architecture

The pipeline extracts raw CSV data (~10 GB), processes it using Apache Spark (PySpark) within an isolated Docker container, and loads it incrementally into a PostgreSQL data warehouse.

```mermaid
graph TD
    subgraph Local Environment
        A[Kaggle CSV Files<br/>Airline Delays]
    end

    subgraph Docker Ecosystem
        B((PySpark Workspace<br/>Python 3.11 + Java 21))
    end

    subgraph PostgreSQL Database
        C[(schema: bronze<br/>Raw Data)]
        D[(schema: silver<br/>Cleaned Data)]
        E[(schema: gold<br/>Aggregated Data)]
    end

    A -->|Volume Mount| B
    B -->|bronze_ingest.py<br/>Append| C
    C -->|silver_clean.py<br/>Incremental Filter & Cast| D
    D -->|gold_aggregate.py<br/>Group By & Math| E
