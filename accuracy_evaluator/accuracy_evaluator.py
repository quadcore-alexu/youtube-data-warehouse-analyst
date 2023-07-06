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
countries = ['Egypt', 'KSA', 'USA', 'Germany']
for i in range(20):
  video_id = random.randint(4, 8) * 10  + random.randint(4, 8)
  country = random.choice(countries)
  gold_table_path = "hdfs://namenode:9000/tmp/gold_countries"
  gold_df = spark.read.format("delta").load(gold_table_path).where((col("channel_id") == channel_id) & (col("user_country") == country))
  json_res = json.loads((gold_df.toJSON().collect())[0])
  gold_views_count = json_res.get('minutes_count')

  bronze_first_views_table = (spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_view_actions"))
  ground_truth = (bronze_views_table
                    .where((col("channel_id") == channel_id) & (col("user_country") == country))
                    .groupBy("channel_id")
                    .agg(count("*").alias("views_count"))
                    .select("channel_id", "views_count")).first()['views_count']
  print(f"{ground_truth}  / {gold_views_count}")
  error += (ground_truth - gold_views_count) / ground_truth * 1.0
error = error / 20.0
print(f"Average Error rate: {error}")
