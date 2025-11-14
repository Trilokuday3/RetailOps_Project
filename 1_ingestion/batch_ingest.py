from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import *

dbutils.widgets.text("source_csv_path", "/Volumes/retailops/default/data_base/sample_sales.csv", "Source CSV File Path")
dbutils.widgets.text("bronze_table_name", "retailops.default.raw_retail", "Bronze Delta Table")
dbutils.widgets.text("quarantine_table_name", "retailops.default.raw_quarantine", "Quarantine Table for Bad Records")

event_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", StringType(), True)
])

corrupt_record_column = "_corrupt_record"

source_path = dbutils.widgets.get("source_csv_path")

try:
    df = (
        spark.read.csv(
            source_path,
            header = True,
            schema = event_schema,
            mode = "PERMISSIVE",
            columnNameOfCorruptRecord = corrupt_record_column
        )
    )

    print(f"Successfully read data from {source_path}")
except Exception as e:
    print(f"Error reading from {source_path}: {e}")
    dbutils.notebook.exit(f"Failed to read source data: {e}")

if corrupt_record_column in df.columns:
    print(f"Corrupt record column '{corrupt_record_column}' found. Splitting good/bad records.")
    good_records = (
        df.filter(col(corrupt_record_column).isNull())
        .withColumn("ingest_time", current_timestamp())
        .drop(corrupt_record_column)
    )

    bad_records = (
        df.filter(col(corrupt_record_column).isNotNull())
        .select(
            col(corrupt_record_column).alias("raw_payload"),
            lit("Corrupt CSV Record").alias("error_message"),
            current_timestamp().alias("ingest_time")
        )
    )
else:
    print(f"No corrupt record column '{corrupt_record_column}' found. Assuming all records are good.")
    good_records = df.withColumn("ingest_time", current_timestamp())
    
    
    bad_records = spark.createDataFrame([],
        StructType([
            StructField("raw_payload", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("ingest_time", TimestampType(), True)
        ])
    )

good_count = good_records.count()
bad_count = bad_records.count()

print(f"Found {good_count} good records and {bad_count} bad records.")

bronze_table_name = dbutils.widgets.get("bronze_table_name")

if good_count > 0:
    try:
        (
            good_records.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table_name)
        )
        print(f"Successfully appended {good_count} good records to Bronze table: {bronze_table_name}")
    except Exception as e:
        print(f"Error writing good records to {bronze_table_name}: {e}")
else:
    print("No good records to write to Bronze table.")


quarantine_table_name = dbutils.widgets.get("quarantine_table_name")

if bad_count > 0:
    try:
        (
            bad_records.write
                .format("delta")
                .mode("append")
                .saveAsTable(quarantine_table_name)  # <-- This is the corrected line
        )
        print(f"Successfully appended {bad_count} bad records to Quarantine table: {quarantine_table_name}")
    except Exception as e:
        print(f"Error writing bad records to {quarantine_table_name}: {e}")
else:
    print("No bad records to write to Quarantine table.")