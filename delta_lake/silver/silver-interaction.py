from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime, timedelta
import time
import pyspark
import os


def write_timestamp_checkpoint(spark, first_views_timestamp, path):
    (spark.createDataFrame([(first_views_timestamp,)], ["last_checked_timestamp"])
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
    silver_table_path = "hdfs://namenode:9000/tmp/silver_interaction"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("views_count", LongType()) \
        .addColumn("channel_id", IntegerType()) \
        .addColumn("video_id", IntegerType()) \
        .addColumn("hour_offset", LongType()) \
        .addColumn("country", StringType()) \
        .location(silver_table_path) \
        .execute()
    silver_table = DeltaTable.forPath(spark, silver_table_path)

    timestamp_checkpoint_path = "hdfs://namenode:9000/tmp/silver_interaction/last_checked_timestamp"
    first_views_start_timestamp = read_timestamp_checkpoint(spark, timestamp_checkpoint_path)

    while True:
        # Read the latest records from the bronze table that satisfy the condition
        bronze_first_views_table = (spark
                                    .read
                                    .format("delta")
                                    .load("hdfs://namenode:9000/tmp/bronze_first_views"))

        first_views_end_timestamp = bronze_first_views_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_first_views_table = bronze_first_views_table.where(
            (col("timestamp_seconds") >= first_views_start_timestamp) & (
                    col("timestamp_seconds") < first_views_end_timestamp)) \
            .withColumn("hour_offset", (floor(col('timestamp_seconds') % lit(86400)) / lit(3600)))

        # Aggregate the data
        aggregated_data = (bronze_first_views_table
                           .groupBy("video_id", "channel_id", "hour_offset", "user_country")
                           .agg(count("*").alias("views_count"))
                           .select("video_id", "hour_offset", "views_count", "channel_id", "user_country"))

        aggregated_data.show()

        # Merge the aggregated data into the silver table
        (silver_table.alias("silver")
         .merge(aggregated_data.alias("bronze"),
                "silver.video_id = bronze.video_id and silver.hour_offset = bronze.hour_offset and "
                "silver.channel_id = bronze.channel_id and silver.country = bronze.user_country")
         .whenMatchedUpdate(set={"views_count": "silver.views_count + bronze.views_count"})
         .whenNotMatchedInsert(values={"hour_offset": "bronze.hour_offset", "video_id": "bronze.video_id",
                                       "channel_id": "bronze.channel_id",
                                       "country": "bronze.user_country",
                                       "views_count": "bronze.views_count"})
         .execute())

        silver_df = spark.read.format("delta").load(silver_table_path)
        silver_df.show()
        first_views_start_timestamp = first_views_end_timestamp

        write_timestamp_checkpoint(spark, first_views_end_timestamp, timestamp_checkpoint_path)

        time.sleep(10)
