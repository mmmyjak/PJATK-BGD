# Data Quality Risks

During the design and implementation of this pipeline, the following three critical data quality risks were identified and addressed:

### 1. Ingestion & Schema Drift Risks (Format Issues)
**The Risk:** The source data provided in CSV format is highly susceptible to structural changes without prior notice. A prime example within this dataset is the presence of an unexpected, empty column (`Unnamed: 27`). 

**Impact:** If the ingestion layer strictly enforces a hardcoded schema, such anomalies would cause pipeline failures or data shifting. This risk necessitates robust schema inference and column dropping capabilities during the transition from the Bronze to the Silver layer.

### 2. Transformation & Completeness Risks (Handling NULLs)
**The Risk:** Raw flight data contains significant logical gaps. For instance, flights that arrived on time often have `NULL` values in their delay columns instead of `0.0`. Furthermore, flights marked as cancelled (`CANCELLED = 1`) lack departure and arrival timestamps.

**Impact:** Performing mathematical aggregations (like averages or sums in the Gold layer) on columns containing unhandled `NULL` values would lead to skewed or completely invalid business metrics. This requires careful coalescing and casting in the Silver layer to ensure mathematical accuracy.

### 3. Scale & Performance Risks (Full Refresh Bottlenecks)
**The Risk:** Processing over 10 GB of data in a single run poses a severe memory and database I/O risk. If the pipeline relies on a "Full Refresh" (truncating and reloading tables) every time a new file is added, the processing time and resource consumption will grow exponentially.

**Impact:** To mitigate this, the pipeline incorporates an **incremental loading strategy** using control log tables (`processed_files_log`). This ensures that only new, unprocessed files are appended and transformed, safeguarding the database against performance degradation and ensuring scalability.