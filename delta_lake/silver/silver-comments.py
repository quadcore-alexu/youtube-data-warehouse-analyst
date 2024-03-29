from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime, timedelta
from params import silver_period
import time
import pyspark
import os


def write_timestamp_checkpoint(spark, comments_timestamp, path):
    (spark.createDataFrame([(comments_timestamp,)],
                           ["last_checked_timestamp"])
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
    silver_table_path = "hdfs://namenode:9000/tmp/silver_comment"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("video_id", IntegerType()) \
        .addColumn("comments_count", LongType()) \
        .addColumn("positive_count", LongType()) \
        .location(silver_table_path) \
        .execute()
    silver_table = DeltaTable.forPath(spark, silver_table_path)

    timestamp_checkpoint_path = "hdfs://namenode:9000/tmp/silver_comment/last_checked_timestamp"
    comments_start_timestamp = read_timestamp_checkpoint(
        spark, timestamp_checkpoint_path)

    while True:
        # Read the latest records from the bronze table that satisfy the condition
        bronze_comments_table = (spark
                                 .read
                                 .format("delta")
                                 .load("hdfs://namenode:9000/tmp/bronze_comments"))

        comments_end_timestamp = bronze_comments_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_comments_table = bronze_comments_table.where((col("timestamp_seconds") >= comments_start_timestamp) & (
                col("timestamp_seconds") < comments_end_timestamp))



        # Aggregate the data
        comments_aggregated_data = (bronze_comments_table
                                    .groupBy("video_id")
                                    .agg(count(when(col("comment_score") == 1, True)).alias("positive_count"),
                                         count(when(col("comment_score") != 0, True)).alias("comments_count"))
                                    .select("video_id", "comments_count", "positive_count"))

        # Merge the aggregated data into the silver table
        (silver_table.alias("silver")
         .merge(comments_aggregated_data.alias("bronze"), "silver.video_id = bronze.video_id")
         .whenMatchedUpdate(set={"comments_count": "silver.comments_count + bronze.comments_count",
                                 "positive_count": "silver.positive_count + bronze.positive_count"
                                 })
         .whenNotMatchedInsert(values={"video_id": "bronze.video_id",
                                       "comments_count": "bronze.comments_count",
                                       "positive_count": "bronze.positive_count"
                                       })
         .execute())

        comments_start_timestamp = comments_end_timestamp

        write_timestamp_checkpoint(
            spark, comments_end_timestamp,
            timestamp_checkpoint_path)

        time.sleep(silver_period)
