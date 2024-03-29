from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime, timedelta
from params import gold_period
import time
import pyspark
import os


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Load the Delta table
    gold_table_path = "hdfs://namenode:9000/tmp/gold_countries"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("channel_id", IntegerType()) \
        .addColumn("country", StringType()) \
        .addColumn("views_count", LongType()) \
        .addColumn("likes_count", LongType()) \
        .addColumn("minutes_count", LongType()) \
        .location(gold_table_path) \
        .execute()
    gold_table = DeltaTable.forPath(spark, gold_table_path)

    while True:
        # Read the latest records from the bronze table that satisfy the condition
        silver_countries_table = (spark
                              .read
                              .format("delta")
                              .load("hdfs://namenode:9000/tmp/silver_countries")
                              .groupBy("channel_id", "country")
                              .agg(sum("views_count").alias("views_count"), sum("likes_count").alias("likes_count"), sum("minutes_count").alias("minutes_count"))
                              .select("channel_id", "country", "views_count", "likes_count", "minutes_count"))

        # Merge the aggregated data into the silver table
        (gold_table.alias("gold")
         .merge(silver_countries_table.alias("silver"), "gold.channel_id = silver.channel_id and gold.country = silver.country")
         .whenMatchedUpdate(set={"views_count": "silver.views_count",
                                 "likes_count": "silver.likes_count",
                                 "minutes_count": "silver.minutes_count"})
         .whenNotMatchedInsert(values={"channel_id": "silver.channel_id",
                                       "country": "silver.country",
                                       "views_count": "silver.views_count",
                                       "likes_count": "silver.likes_count",
                                       "minutes_count": "silver.minutes_count"})
         .execute())

        time.sleep(gold_period)
