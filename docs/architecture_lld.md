# Pipeline Architecture LLD

The pipeline extracts raw CSV data (~10 GB), processes it using Apache Spark (PySpark) within an isolated Docker container, and loads it incrementally into a PostgreSQL data warehouse. The workflow is orchestrated by Apache Airflow and additionally supports a Kafka-triggered DAG path for automatic starts.

```mermaid
graph TD
    subgraph Orchestration Layer
        Airflow[Apache Airflow<br/>DAG: medallion_airline_pipeline]
        AirflowKafka[Apache Airflow<br/>DAG: medallion_airline_pipeline_kafka]
    end

    subgraph Local Environment
        A[Kaggle CSV Files<br/>Airline Delays]
    end

    subgraph Event Layer
        KP[Kafka Source Producer]
        K[(Apache Kafka)]
        KT[Kafka Airflow Trigger]
    end

    subgraph Execution Environment
        subgraph PySpark Container [bgd_pyspark]
            B((PySpark<br/>Python 3.11 + Java 21))
            DBT[dbt-postgres<br/>Incremental Models]
        end
    end

    subgraph PostgreSQL Database
        C[(schema: bronze<br/>Raw Data)]
        D[(schema: silver<br/>Cleaned Data)]
        E[(schema: gold<br/>Aggregated Data)]
    end

    A -->|Volume Mount| B
    A -->|File Events| KP
    KP --> K
    K --> KT
    KT -.->|trigger DAG run| AirflowKafka

    %% Control Flow (Orchestration)
    Airflow -.->|1. docker exec bronze_ingest.py| B
    Airflow -.->|2. docker exec silver_clean.py| B
    Airflow -.->|3. docker exec dbt run| DBT
    Airflow -.->|4. docker exec dbt test| DBT
    AirflowKafka -.->|1. docker exec bronze_ingest.py target_file| B
    AirflowKafka -.->|2. docker exec silver_clean.py| B
    AirflowKafka -.->|3. docker exec dbt run| DBT
    AirflowKafka -.->|4. docker exec dbt test| DBT

    %% Data Flow (ELT Process)
    B ==>|Ingest & Append| C
    C ==>|PySpark Reads & Cleans| D
    D ==>|dbt Reads & Aggregates<br/>delete+insert incremental| E
```