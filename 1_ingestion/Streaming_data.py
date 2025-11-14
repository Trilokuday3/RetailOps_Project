# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

dbutils.widgets.text("kafka_bootstrap_servers", "your_kafka_server:9092", "Kafka Bootstrap Servers")
dbutils.widgets.text("kafka_topic", "retail_events", "Kafka Topic Name")
dbutils.widgets.text("bronze_table_name", "retailops.default.raw_retail", "Bronze Delta Table Name")
dbutils.widgets.text("quarantine_table_name", "retailops.default.raw_quarantine", "Quarantine Table for Bad Records")
dbutils.widgets.text("checkpoint_location_bronze", "/Volumes/retailops/default/checkpoints/streaming_bronze", "Streaming Checkpoint (Bronze)")
dbutils.widgets.text("checkpoint_location_quarantine", "/Volumes/retailops/default/checkpoints/streaming_quarantine", "Streaming Checkpoint (Quarantine)")

# === ADD WIDGETS FOR AUTHENTICATION ===
dbutils.widgets.text("kafka_api_key", "YOUR_API_KEY", "Kafka API Key (Username)")
dbutils.widgets.text("kafka_api_secret", "YOUR_API_SECRET", "Kafka API Secret (Password)")

# COMMAND ----------
# DBTITLE 3,Define Schema for Kafka Payload
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

# COMMAND ----------
# DBTITLE 4,Read from Kafka Stream
kafka_server = dbutils.widgets.get("kafka_bootstrap_servers")
kafka_topic = dbutils.widgets.get("kafka_topic")

# === GET AUTH PARAMETERS ===
api_key = dbutils.widgets.get("kafka_api_key")
api_secret = dbutils.widgets.get("kafka_api_secret")
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{api_key}' password='{api_secret}';"

try:
    kafka_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", kafka_topic)
            .option("failOnDataLoss", "false")
            # === ADD AUTHENTICATION OPTIONS ===
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas_config)
            .load()
    )
    print("Kafka stream successfully initiated.")

except Exception as e:
    print(f"Error connecting to Kafka stream: {e}")
    dbutils.notebook.exit(f"Failed to read from Kafka: {e}")

# COMMAND ----------
# DBTITLE 5,Parse and Separate Good vs. Bad Records
# (This code remains the same)
parsed_stream = (
    kafka_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("value").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp")
    )
)
good_records = (
    parsed_stream.filter(col("data").isNotNull())
                 .select("data.*")
                 .withColumn("ingest_time", current_timestamp())
)
bad_records = (
    parsed_stream.filter(col("data").isNull())
                 .select(
                     col("raw_payload").cast("string"),
                     col("kafka_timestamp"),
                     lit("Malformed JSON").alias("error_reason"),
                     current_timestamp().alias("ingest_time")
                 )
)
print("Stream parsing and quarantine logic is defined.")

# COMMAND ----------
# DBTITLE 6,Write Good Records to Bronze Table (Stream 1)
# (This code remains the same)
bronze_table = dbutils.widgets.get("bronze_table_name")
checkpoint_bronze = dbutils.widgets.get("checkpoint_location_bronze")

try:
    (
        good_records.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_bronze)
            .trigger(availableNow=True)
            .toTable(bronze_table)
    )
    print(f"Streaming write of good records to {bronze_table} has started.")
except Exception as e:
    print(f"Error writing good records stream: {e}")

# COMMAND ----------
# DBTITLE 7,Write Bad Records to Quarantine Table (Stream 2)
# (This code remains the same)
quarantine_table = dbutils.widgets.get("quarantine_table_name")
checkpoint_quarantine = dbutils.widgets.get("checkpoint_location_quarantine")

try:
    (
        bad_records.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_quarantine)
            .trigger(availableNow=True)
            .toTable(quarantine_table)
    )
    print(f"Streaming write of bad records to {quarantine_table} has started.")
except Exception as e:
    print(f"Error writing bad records stream: {e}")