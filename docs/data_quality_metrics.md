# Data Quality Metrics - Airline Delays Data Product

Scope: dbt Gold model `gold_gold.monthly_carrier_performance`

| Metric Name | Metric Definition | Current Value | Expected Threshold | Update Cadence |
|---|---|---:|---|---|
| Validity - Test execution reliability | dbt tests for `gold_gold.monthly_carrier_performance` that passed in the latest run. | 100.0% pass rate | 100% | Every pipeline run |
| Uniqueness - Carrier-Month key uniqueness | Percentage of distinct (`op_carrier`, `flight_month`, `flight_year`) combinations in `gold_gold.monthly_carrier_performance` compared to total rows. | 100.0% | 100% | Every pipeline run |
| Completeness - Required fields | Percentage of rows where `flight_year`, `flight_month`, and `op_carrier` are all non-null in `gold_gold.monthly_carrier_performance`. (`avg_departure_delay` can be null if there was no delays)| 100.0% | 100% | Every pipeline run |