from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime, timedelta
import time
import pyspark
import os


def write_timestamp_checkpoint(spark, first_views_timestamp, likes_timestamp, view_actions_timestamp,
                               subscribes_timestamp, path):
    (spark.createDataFrame([(first_views_timestamp, likes_timestamp, view_actions_timestamp, subscribes_timestamp,)],
                           ["last_checked_timestamp"])
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(path))


def read_timestamp_checkpoint(spark, path):
    if not os.path.exists(path):
        return 1683694784, 1683694784, 1683694784
    checkpoint_timestamp_df = (spark
                               .read
                               .option("header", "true")
                               .csv(path)
                               .select("last_checked_timestamp"))

    return int(checkpoint_timestamp_df.first()[0]), int(checkpoint_timestamp_df.first()[1]), int(
        checkpoint_timestamp_df.first()[2]), int(checkpoint_timestamp_df.first()[3])



if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Load the Delta table
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_hour_channel"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("channel_id", IntegerType()) \
        .addColumn("views_count", LongType()) \
        .addColumn("likes_count", LongType()) \
        .addColumn("minutes_count", LongType()) \
        .location(gold_table_path) \
        .execute()
    gold_table = DeltaTable.forPath(spark, gold_table_path)

    while True:
        current_hour = int(datetime.timestamp(datetime.now()))
        current_hour = current_hour - (current_hour % 3600)
        # Read the latest records from the bronze table that satisfy the condition
        silver_video_table = (spark
                              .read
                              .format("delta")
                              .load("hdfs://namenode:9000/tmp/silver_channel")
                              .where((col("hour_timestamp") >= current_hour))
                              .groupBy("channel_id")
                              .agg(sum("views_count").alias("views_count"), sum("likes_count").alias("likes_count"),
                                   sum("minutes_count").alias("minutes_count"))
                              .select("channel_id", "views_count", "likes_count", "minutes_count"))

        # Merge the aggregated data into the silver table
        (gold_table.alias("gold")
         .merge(silver_video_table.alias("silver"), "gold.channel_id = silver.channel_id")
         .whenMatchedUpdate(set={"views_count": "silver.views_count",
                                 "likes_count": "silver.likes_count",
                                 "minutes_count": "silver.minutes_count"})
         .whenNotMatchedInsert(values={"channel_id": "silver.channel_id",
                                       "views_count": "silver.views_count",
                                       "likes_count": "silver.likes_count",
                                       "minutes_count": "silver.minutes_count"})
         .execute())

        gold_df = spark.read.format("delta").load(gold_table_path)
        gold_df.show()

        time.sleep(60)
