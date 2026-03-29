import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, coalesce, lit
from pyspark.sql.types import IntegerType, DoubleType

load_dotenv()

RUN_PROFILE = "demo"
# RUN_PROFILE = "full"

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

if RUN_PROFILE == "demo":
    SILVER_SAMPLE_ROWS = 500
    SILVER_MAX_FILES = 1
else:
    SILVER_SAMPLE_ROWS = 0
    SILVER_MAX_FILES = 0

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

spark = SparkSession.builder \
    .appName("Airlines-Silver-Transform-Incremental") \
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


def process_single_file(file_name):
    safe_file_name = file_name.replace("'", "''")
    base_query = f"SELECT * FROM bronze.flights WHERE _source_file = '{safe_file_name}'"
    if SILVER_SAMPLE_ROWS > 0:
        base_query = f"{base_query} LIMIT {SILVER_SAMPLE_ROWS}"
    query = f"({base_query}) AS bronze_file"

    read_options = dict(DB_PROPERTIES)
    read_options["fetchsize"] = "50000"

    df_bronze = spark.read.jdbc(
        url=JDBC_URL,
        table=query,
        properties=read_options
    )

    if "Unnamed: 27" in df_bronze.columns:
        df_bronze = df_bronze.drop("Unnamed: 27")

    df_casted = df_bronze \
        .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd")) \
        .withColumn("DEP_DELAY", col("DEP_DELAY").cast(DoubleType())) \
        .withColumn("ARR_DELAY", col("ARR_DELAY").cast(DoubleType())) \
        .withColumn("CANCELLED", col("CANCELLED").cast(DoubleType())) \
        .withColumn("DIVERTED", col("DIVERTED").cast(DoubleType()))

    delay_columns = [
        "CARRIER_DELAY",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "SECURITY_DELAY",
        "LATE_AIRCRAFT_DELAY"
    ]

    df_filled = df_casted
    for column in delay_columns:
        df_filled = df_filled.withColumn(
            column,
            coalesce(col(column).cast(DoubleType()), lit(0.0))
        )

    df_silver = df_filled.withColumn("_transformed_at", current_timestamp())

    writer_options = dict(DB_PROPERTIES)
    writer_options["batchsize"] = "20000"

    df_silver.write.jdbc(
        url=JDBC_URL,
        table="silver.flights",
        mode="append",
        properties=writer_options
    )

    if SILVER_SAMPLE_ROWS > 0:
        return

    df_log = spark.createDataFrame([(file_name,)], ["file_name"]) \
        .withColumn("processed_at", current_timestamp())

    df_log.write.jdbc(
        url=JDBC_URL,
        table="silver.processed_files_log",
        mode="append",
        properties=DB_PROPERTIES
    )

def main():
    bronze_files = get_processed_files("bronze.processed_files_log")
    silver_files = get_processed_files("silver.processed_files_log")
    
    files_to_process = [f for f in bronze_files if f not in silver_files]

    if not files_to_process:
        print("No new files to process for Silver layer.")
        return

    if SILVER_MAX_FILES > 0:
        files_to_process = files_to_process[:SILVER_MAX_FILES]

    print(f"Found {len(files_to_process)} files for Silver processing.")
    for file_name in files_to_process:
        print(f"Processing Silver file: {file_name} ...")
        process_single_file(file_name)
        print(f"Finished Silver file: {file_name}")

if __name__ == "__main__":
    main()
    spark.stop()