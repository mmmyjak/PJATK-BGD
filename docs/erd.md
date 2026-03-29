# Entity-Relationship Diagram (ERD)

This diagram represents the data model and schema evolution across the Medallion Architecture (Bronze, Silver, and Gold layers) implemented in PostgreSQL.

```mermaid
erDiagram
    BRONZE_FLIGHTS {
        string FL_DATE
        string OP_CARRIER
        string DEP_DELAY
        string UNNAMED_27 "Dirty column"
        string _source_file
        timestamp _ingested_at
    }

    SILVER_FLIGHTS {
        date FL_DATE
        string OP_CARRIER
        float DEP_DELAY
        int CANCELLED
        string _source_file
        timestamp _transformed_at
    }

    GOLD_MONTHLY_CARRIER_PERFORMANCE {
        int flight_year
        int flight_month
        string OP_CARRIER
        int total_flights
        float avg_departure_delay
        timestamp _aggregated_at
    }
    
    GOLD_YEARLY_ROUTE_RELIABILITY {
        int flight_year
        string ORIGIN
        string DEST
        int total_flights_on_route
        float max_arrival_delay
        timestamp _aggregated_at
    }

    BRONZE_FLIGHTS ||--o| SILVER_FLIGHTS : "Cleansed & Typed"
    SILVER_FLIGHTS ||--o{ GOLD_MONTHLY_CARRIER_PERFORMANCE : "Aggregated by Carrier"
    SILVER_FLIGHTS ||--o{ GOLD_YEARLY_ROUTE_RELIABILITY : "Aggregated by Route"