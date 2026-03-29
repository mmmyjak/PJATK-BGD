import os
import glob
from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

spark = SparkSession.builder \
    .appName("Airlines-Bronze-Ingestion") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def get_processed_files():
    try:
        df_processed = spark.read.jdbc(
            url=JDBC_URL,
            table="bronze.processed_files_log",
            properties=DB_PROPERTIES
        )
        return [row["file_name"] for row in df_processed.collect()]
    except Exception as e:
        print("Processed files log table not found or error occurred. Assuming no files have been processed yet.")
        return []

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(script_dir, "../data/source")
    source_dir = os.path.abspath(source_dir)
    
    all_files = glob.glob(os.path.join(source_dir, "*.csv"))
    
    if not all_files:
        print(f"No files found in folder {source_dir}. Exiting.")
        return

    processed_files = get_processed_files()
    files_to_process = [f for f in all_files if os.path.basename(f) not in processed_files]

    if not files_to_process:
        print("All files in the folder have already been processed. Nothing to do!")
        return

    print(f"Found {len(files_to_process)} new files to process.")

    for file_path in files_to_process:
        file_name = os.path.basename(file_path)
        print(f"Processing file: {file_name} ...")

        df_raw = spark.read.csv(file_path, header=True, inferSchema=False)

        df_bronze = df_raw \
            .withColumn("_source_file", lit(file_name)) \
            .withColumn("_ingested_at", current_timestamp())

        df_bronze.write.jdbc(
            url=JDBC_URL,
            table="bronze.flights",
            mode="append",
            properties=DB_PROPERTIES
        )

        df_log = spark.createDataFrame([(file_name,)], ["file_name"]) \
            .withColumn("processed_at", current_timestamp())

        df_log.write.jdbc(
            url=JDBC_URL,
            table="bronze.processed_files_log",
            mode="append",
            properties=DB_PROPERTIES
        )
        
        print(f"Finished writing: {file_name}")

    print("Bronze pipeline completed successfully!")

if __name__ == "__main__":
    main()
    spark.stop()