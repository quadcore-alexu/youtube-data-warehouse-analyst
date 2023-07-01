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
    gold_table_path = "hdfs://namenode:9000/tmp/gold_interaction"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("max_interaction_time", LongType()) \
        .addColumn("channel_id", IntegerType()) \
        .addColumn("country", StringType()) \
        .location(gold_table_path) \
        .execute()
    gold_table = DeltaTable.forPath(spark, gold_table_path)

    while True:
        # Read the latest records from the silver table that satisfy the condition
        silver_interaction_table = (spark
                                    .read
                                    .format("delta")
                                    .load("hdfs://namenode:9000/tmp/silver_interaction"))

        aggregated_data = (silver_interaction_table.alias("silver")
                           .withColumn("interaction", struct("silver.views_count", "silver.hour_offset"))
                           .groupBy("channel_id", "country")
                           .agg(max(col("interaction")).alias("interaction"))
                           .withColumn("views_count", col("interaction.views_count"))
                           .withColumn("hour_offset", col("interaction.hour_offset"))
                           .select("channel_id", "country", "hour_offset"))

        # Merge the aggregated data into the gold table
        (gold_table.alias("gold")
         .merge(silver_interaction_table.alias("silver"),
                "gold.channel_id = silver.channel_id and gold.country = silver.country")
         .whenMatchedUpdate(set={"hour_offset": "silver.hour_offset"})
         .whenNotMatchedInsert(values={"channel_id": "silver.cannel_id",
                                       "country": "silver.country",
                                       "hour_offset": "silver.hour_offset"})
         .execute())

        gold_df = spark.read.format("delta").load(gold_table_path)
        gold_df.show()

        time.sleep(60)
