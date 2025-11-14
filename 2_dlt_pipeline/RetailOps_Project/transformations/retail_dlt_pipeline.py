import dlt
from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.types import *

@dlt.table(
    name= "raw_detail_dlt",
    comment = "Raw retail data ingested form Stage 1. Read as a stream."
)
def raw_detail_dlt():
    return (
        spark.readStream.table("retailops.default.raw_retail")
    )

silver_schema = StructType([
    StructField("order_id", StringType(), False), 
    StructField("event_time", TimestampType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("payment_method", StringType(), True)
])

silver_dq_rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "valid_price": "price >= 0"
}

@dlt.table
@dlt.expect_all(silver_dq_rules)
def clean_retail():
    return (
        dlt.read_stream("raw_detail_dlt")
            .select(
                col("order_id"),
                to_timestamp(col("timestamp")).alias("event_time"),
                col("product").cast(StringType()), 
                col("quantity").cast(IntegerType()), 
                col("price").cast(DoubleType()), 
                col("store_id").cast(StringType()),
                col("payment_method").cast(StringType())
           )
           .dropDuplicates(["order_id"])
    )

@dlt.table
@dlt.expect("non_negative_sales", "total_sales >= 0")
def retail_metrics():
    return (
        dlt.read("clean_retail")
        .withColumn("total_sales", col("quantity") * col("price"))
        .groupby(col("event_time").cast("date").alias("date"), col("store_id"))
        .agg(
            expr("sum(total_sales)").alias("total_sales"),
            expr("sum(quantity)").alias("units_sold")
        )
    )