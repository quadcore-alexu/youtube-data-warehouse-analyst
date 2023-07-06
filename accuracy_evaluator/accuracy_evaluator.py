import pyspark
from delta import *
from pyspark.sql.functions import *
import json
import pyspark.sql.functions as fn
from pyspark.sql.types import *
import random

builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

error = 0
for i in range(20):
  gold_table_all = "hdfs://namenode:9000/tmp/gold_alltime_video"
  video_id = random.randint(4, 8) * 10  + random.randint(4, 8)
  all_df = spark.read.format("delta").load(gold_table_all).where((col("video_id") == video_id))
  json_res = json.loads((all_df.toJSON().collect())[0])
  gold_views_count = json_res.get('views_count')

  bronze_first_views_table = (spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_first_views"))
  ground_truth = (bronze_first_views_table
                    .groupBy("video_id")
                    .agg(count("*").alias("views_count"))
                    .select("video_id", "views_count")).first()['views_count']
  error += (ground_truth - gold_views_count) / ground_truth * 1.0
error = error / 20.0
print(f"Average Error rate: {error}")
