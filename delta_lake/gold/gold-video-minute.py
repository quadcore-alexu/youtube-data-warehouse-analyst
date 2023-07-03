from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
import time
import pyspark
import os


def write_timestamp_checkpoint(spark, likes_timestamp, view_actions_timestamp, path):
    (spark.createDataFrame([(likes_timestamp, view_actions_timestamp,)],
                           ["last_checked_timestamp"])
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(path))


def read_timestamp_checkpoint(spark, path):
    if not os.path.exists(path):
        return 1683694784, 1683694784
    checkpoint_timestamp_df = (spark
                               .read
                               .option("header", "true")
                               .csv(path)
                               .select("last_checked_timestamp"))

    return int(checkpoint_timestamp_df.first()[0]), int(checkpoint_timestamp_df.first()[1])


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the Delta table
    gold_table_path = "hdfs://namenode:9000/tmp/gold_video_minute"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("video_id", IntegerType()) \
        .addColumn("views_count", LongType()) \
        .addColumn("likes_count", LongType()) \
        .addColumn("minutes_offset", LongType()) \
        .location(gold_table_path) \
        .execute()
    gold_table = DeltaTable.forPath(spark, gold_table_path)

    timestamp_checkpoint_path = "hdfs://namenode:9000/tmp/gold_video_minute/last_checked_timestamp"
    likes_start_timestamp, view_actions_start_timestamp = read_timestamp_checkpoint(
        spark, timestamp_checkpoint_path)

    while True:
        # Read the latest records from the bronze table that satisfy the condition

        bronze_likes_table = (spark
                              .read
                              .format("delta")
                              .load("hdfs://namenode:9000/tmp/bronze_likes"))

        likes_end_timestamp = bronze_likes_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_likes_table = bronze_likes_table.where((col("timestamp_seconds") >= likes_start_timestamp) & (
                col("timestamp_seconds") < likes_end_timestamp))

        bronze_view_actions_table = (spark
                                     .read
                                     .format("delta")
                                     .load("hdfs://namenode:9000/tmp/bronze_view_actions"))

        view_actions_end_timestamp = bronze_view_actions_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_view_actions_table = bronze_view_actions_table.where(
            (col("timestamp_seconds") >= view_actions_start_timestamp) & (
                    col("timestamp_seconds") < view_actions_end_timestamp))

        # Aggregate the data

        likes_aggregated_data = (bronze_likes_table
                                 .groupBy("video_id", "seconds_offset")
                                 .agg(count("*").alias("likes_count"))
                                 .select("video_id", "seconds_offset", "likes_count"))

        views_aggregated_data = (bronze_view_actions_table
                                 .groupBy("video_id", "seconds_offset")
                                 .agg(count("*").alias("views_count"))
                                 .select("video_id", "seconds_offset", "views_count"))

        aggregated_data = likes_aggregated_data.join(views_aggregated_data, ['video_id', 'seconds_offset'], 'left')

        aggregated_data.show()

        # Merge the aggregated data into the silver table
        (gold_table.alias("gold")
         .merge(aggregated_data.alias("bronze"),
                "gold.video_id = bronze.video_id and gold.minutes_offset = bronze.seconds_offset")
         .whenMatchedUpdate(set={"views_count": "gold.views_count + bronze.views_count",
                                 "likes_count": "gold.likes_count + bronze.likes_count"})
         .whenNotMatchedInsert(values={"minutes_offset": "bronze.seconds_offset", "video_id": "bronze.video_id",
                                       "views_count": "bronze.views_count",
                                       "likes_count": "bronze.likes_count"})
         .execute())

        gold_df = spark.read.format("delta").load(gold_table_path)
        gold_df.show()
        likes_start_timestamp = likes_end_timestamp
        view_actions_start_timestamp = view_actions_end_timestamp

        write_timestamp_checkpoint(
            spark, likes_end_timestamp, view_actions_end_timestamp,
            timestamp_checkpoint_path)

        time.sleep(10)
