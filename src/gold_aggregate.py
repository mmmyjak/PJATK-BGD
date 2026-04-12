import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, sum, avg, max, current_timestamp

load_dotenv()

# was replaced with data build tool

RUN_PROFILE = "demo"
# RUN_PROFILE = "full"

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

if RUN_PROFILE == "demo":
    GOLD_MAX_FILES = 1
else:
    GOLD_MAX_FILES = 0

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

spark = SparkSession.builder \
    .appName("Airlines-Gold-Aggregate-Incremental") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def get_processed_files(table_name):
    try:
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=table_name,
            properties=DB_PROPERTIES
        )
        return [row["file_name"] for row in df.collect()]
    except Exception:
        return []


def get_silver_source_files():
    try:
        df = spark.read.jdbc(
            url=JDBC_URL,
            table="(SELECT DISTINCT _source_file AS file_name FROM silver.flights) AS silver_source_files",
            properties=DB_PROPERTIES
        )
        return [row["file_name"] for row in df.collect()]
    except Exception:
        return []


def process_single_file(file_name):
    safe_file_name = file_name.replace("'", "''")
    df_silver = spark.read.jdbc(
        url=JDBC_URL,
        table=f"(SELECT * FROM silver.flights WHERE _source_file = '{safe_file_name}') AS silver_file",
        properties=DB_PROPERTIES
    )

    df_base = df_silver \
        .withColumn("flight_year", year(col("FL_DATE"))) \
        .withColumn("flight_month", month(col("FL_DATE")))

    df_carrier_performance = df_base.groupBy("flight_year", "flight_month", "OP_CARRIER").agg(
        count("*").alias("total_flights"),
        sum("CANCELLED").alias("cancelled_flights"),
        avg("DEP_DELAY").alias("avg_departure_delay"),
        sum("WEATHER_DELAY").alias("total_weather_delay_mins")
    ).withColumn("_aggregated_at", current_timestamp())

    df_carrier_performance.write.jdbc(
        url=JDBC_URL,
        table="gold.monthly_carrier_performance",
        mode="append",
        properties=DB_PROPERTIES
    )

    df_route_reliability = df_base.groupBy("flight_year", "ORIGIN", "DEST").agg(
        count("*").alias("total_flights_on_route"),
        avg(col("AIR_TIME").cast("double")).alias("avg_air_time"),
        max("ARR_DELAY").alias("max_arrival_delay")
    ).withColumn("_aggregated_at", current_timestamp())

    df_route_reliability.write.jdbc(
        url=JDBC_URL,
        table="gold.yearly_route_reliability",
        mode="append",
        properties=DB_PROPERTIES
    )

    if RUN_PROFILE == "demo":
        return

    df_log = spark.createDataFrame([(file_name,)], ["file_name"]) \
        .withColumn("processed_at", current_timestamp())

    df_log.write.jdbc(
        url=JDBC_URL,
        table="gold.processed_files_log",
        mode="append",
        properties=DB_PROPERTIES
    )

def main():
    if RUN_PROFILE == "demo":
        silver_files = get_silver_source_files()
    else:
        silver_files = get_processed_files("silver.processed_files_log")

    gold_files = get_processed_files("gold.processed_files_log")
    
    files_to_process = [f for f in silver_files if f not in gold_files]

    if not files_to_process:
        print("No new files to process for Gold layer.")
        return

    if GOLD_MAX_FILES > 0:
        files_to_process = files_to_process[:GOLD_MAX_FILES]

    print(f"Found {len(files_to_process)} files for Gold processing.")
    for file_name in files_to_process:
        print(f"Processing Gold file: {file_name} ...")
        process_single_file(file_name)
        print(f"Finished Gold file: {file_name}")

if __name__ == "__main__":
    main()
    spark.stop()