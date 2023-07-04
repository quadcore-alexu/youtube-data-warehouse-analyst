import os
import time
from datetime import datetime

import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, IntegerType, FloatType


def write_timestamp_checkpoint(spark, comments_timestamp, path):
    (spark.createDataFrame([(comments_timestamp,)], ["last_checked_timestamp"])
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
    gold_table_path = "hdfs://namenode:9000/tmp/gold_comments"
    DeltaTable.createIfNotExists(spark) \
        .addColumn("video_id", IntegerType()) \
        .addColumn("ratio", FloatType()) \
        .location(gold_table_path) \
        .execute()
    gold_table = DeltaTable.forPath(spark, gold_table_path)

    while True:
        silver_video_table = (spark
                              .read
                              .format("delta")
                              .load("hdfs://namenode:9000/tmp/silver_comment"))

        # Merge the aggregated data into the silver table
        (gold_table.alias("gold")
         .merge(silver_video_table.alias("silver"), "gold.video_id = silver.video_id")
         .whenMatchedUpdate(set={"ratio": "silver.positive_count / silver.comments_count * 1.0"})
         .whenNotMatchedInsert(values={"video_id": "silver.video_id",
                                       "ratio": "silver.positive_count / silver.comments_count"
                                       })
         .execute())

        gold_df = spark.read.format("delta").load(gold_table_path)
        gold_df.show()

        time.sleep(60)
