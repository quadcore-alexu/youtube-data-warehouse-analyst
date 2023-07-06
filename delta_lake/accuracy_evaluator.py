import pyspark
from delta import *
from pyspark.sql.functions import *
import json
import pyspark.sql.functions as fn
from pyspark.sql.types import *
import random

for i in range(20):
  video_id = random.randint(1, 10) * 10  + random.randint(1, 10)
  all_df = spark.read.format("delta").load(gold_table_all).where((col("video_id") == video_id))
  gold_views_count = json.loads((all_df.toJSON().collect())[0])['views_count']


  bronze_first_views_table = (spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_first_views"))
  ground_truth = (bronze_first_views_table
                    .groupBy("video_id")
                    .agg(count("*").alias("views_count"))
                    .select("video_id", "views_count")).first()['views_count']
  error += (ground_truth - gold_views_count) / ground_truth * 1.0
error = error / 20.0
print(f"Average Error rate: {error}")
