from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime, timedelta
import time
import pyspark
import os
from params import silver_period


def write_timestamp_checkpoint(spark, first_views_timestamp, likes_timestamp, view_actions_timestamp, path):
    (spark.createDataFrame([(first_views_timestamp, likes_timestamp, view_actions_timestamp,)],
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
        checkpoint_timestamp_df.first()[2])


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the Delta table
    silver_table_path = "hdfs://namenode:9000/tmp/silver_video_1"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("hour_timestamp", LongType()) \
        .addColumn("video_id", IntegerType()) \
        .addColumn("views_count", LongType()) \
        .addColumn("likes_count", LongType()) \
        .addColumn("minutes_count", LongType()) \
        .location(silver_table_path) \
        .execute()
    silver_table = DeltaTable.forPath(spark, silver_table_path)

    timestamp_checkpoint_path = "hdfs://namenode:9000/tmp/silver_video_1/last_checked_timestamp"
    first_views_start_timestamp, likes_start_timestamp, view_actions_start_timestamp = read_timestamp_checkpoint(
        spark, timestamp_checkpoint_path)

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
                    col("timestamp_seconds") < first_views_end_timestamp)).withColumn("hour_timestamp_seconds",
                                                                                      col('timestamp_seconds') - col(
                                                                                          'timestamp_seconds') % lit(
                                                                                          3600))

        bronze_likes_table = (spark
                              .read
                              .format("delta")
                              .load("hdfs://namenode:9000/tmp/bronze_likes"))

        likes_end_timestamp = bronze_likes_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_likes_table = bronze_likes_table.where((col("timestamp_seconds") >= likes_start_timestamp) & (
                col("timestamp_seconds") < likes_end_timestamp)).withColumn("hour_timestamp_seconds",
                                                                            col('timestamp_seconds') - col(
                                                                                'timestamp_seconds') % lit(3600))

        bronze_view_actions_table = (spark
                                     .read
                                     .format("delta")
                                     .load("hdfs://namenode:9000/tmp/bronze_view_actions"))

        view_actions_end_timestamp = bronze_view_actions_table.agg(
            max(col("timestamp_seconds"))).collect()[0][0]

        bronze_view_actions_table = bronze_view_actions_table.where(
            (col("timestamp_seconds") >= view_actions_start_timestamp) & (
                    col("timestamp_seconds") < view_actions_end_timestamp)).withColumn("hour_timestamp_seconds",
                                                                                       col('timestamp_seconds') - col(
                                                                                           'timestamp_seconds') % lit(
                                                                                           3600))

        # Aggregate the data
        first_views_aggregated_data = (bronze_first_views_table
                                       .groupBy("video_id", "hour_timestamp_seconds")
                                       .agg(count("*").alias("views_count"))
                                       .select("video_id", "hour_timestamp_seconds", "views_count"))

        likes_aggregated_data = (bronze_likes_table
                                 .groupBy("video_id", "hour_timestamp_seconds")
                                 .agg(count("*").alias("likes_count"))
                                 .select("video_id", "hour_timestamp_seconds", "likes_count"))

        views_aggregated_data = (bronze_view_actions_table
                                 .groupBy("video_id", "hour_timestamp_seconds")
                                 .agg(count("*").alias("minutes_count"))
                                 .select("video_id", "hour_timestamp_seconds", "minutes_count"))

        aggregated_data = first_views_aggregated_data.join(
            likes_aggregated_data, ['video_id', 'hour_timestamp_seconds'], 'left').join(views_aggregated_data, [
            'video_id', 'hour_timestamp_seconds'], 'left')

        # Merge the aggregated data into the silver table
        (silver_table.alias("silver")
         .merge(aggregated_data.alias("bronze"),
                "silver.video_id = bronze.video_id and silver.hour_timestamp = bronze.hour_timestamp_seconds")
         .whenMatchedUpdate(set={"views_count": "silver.views_count + bronze.views_count",
                                 "likes_count": "silver.likes_count + bronze.likes_count",
                                 "minutes_count": "silver.minutes_count + bronze.minutes_count"})
         .whenNotMatchedInsert(values={"hour_timestamp": "bronze.hour_timestamp_seconds", "video_id": "bronze.video_id",
                                       "views_count": "bronze.views_count",
                                       "likes_count": "bronze.likes_count",
                                       "minutes_count": "bronze.minutes_count"})
         .execute())

        first_views_start_timestamp = first_views_end_timestamp
        likes_start_timestamp = likes_end_timestamp
        view_actions_start_timestamp = view_actions_end_timestamp

        write_timestamp_checkpoint(
            spark, first_views_end_timestamp, likes_end_timestamp, view_actions_end_timestamp,
            timestamp_checkpoint_path)

        time.sleep(silver_period)
