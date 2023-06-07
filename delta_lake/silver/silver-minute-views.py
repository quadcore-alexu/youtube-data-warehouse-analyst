from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import time
import pyspark
import os


def write_timestamp_checkpoint(spark, timestamp, path):
    (spark.createDataFrame([(timestamp,)], ["last_checked_timestamp"])
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(path))


def read_timestamp_checkpoint(spark, path):
    if not os.path.exists(path):
        return 1683694784
    checkpoint_timestamp_df = (spark
                               .read
                               .option("header", "true")
                               .csv(path)
                               .select("last_checked_timestamp"))

    return int(checkpoint_timestamp_df.first()[0])


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the Delta table
    silver_table_path = "hdfs://namenode:9000/tmp/silver_23"
    bronze_table_path = "hdfs://namenode:9000/tmp/bronze_16"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("country", StringType()) \
        .addColumn("views_count", IntegerType()) \
        .location(silver_table_path) \
        .execute()
    silver_table = DeltaTable.forPath(spark, silver_table_path)

    timestamp_checkpoint_path = "hdfs://namenode:9000/tmp/silver_17/last_checked_timestamp"
    start_timestamp = read_timestamp_checkpoint(
        spark, timestamp_checkpoint_path)

    while True:
        # Read the latest records from the bronze table that satisfy the condition
        bronze_table = (spark
                        .read
                        .format("delta")
                        .load(bronze_table_path))

        end_timestamp = bronze_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        print(start_timestamp, "  --  ", end_timestamp)
        bronze_table = bronze_table.where((col("timestamp_seconds") >= start_timestamp) & (
            col("timestamp_seconds") < end_timestamp))

        # Aggregate the data
        aggregated_data = (bronze_table
                           .groupBy("user_country")
                           .agg(count("*").alias("total_views_count"))
                           .select("user_country", "total_views_count"))

        # Merge the aggregated data into the silver table
        (silver_table.alias("silver")
         .merge(aggregated_data.alias("bronze"), "silver.country = bronze.user_country")
         .whenMatchedUpdate(set={"views_count": "views_count + bronze.total_views_count"})
         .whenNotMatchedInsert(values={"country": "bronze.user_country", "views_count": "bronze.total_views_count"})
         .execute())

        silver_df = spark.read.format("delta").load(silver_table_path)
        silver_df.show()
        start_timestamp = end_timestamp

        bronze_table = (spark
                        .read
                        .format("delta")
                        .load(bronze_table_path)
                        .orderBy(col("timestamp_seconds").desc()))
        print("Real Count ", bronze_table.count())
        write_timestamp_checkpoint(
            spark, end_timestamp, timestamp_checkpoint_path)

        time.sleep(10)
