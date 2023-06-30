import pyspark
from delta import *
import json
from flask import Flask, jsonify


app = Flask(__name__)
builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
    "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


@app.route('/')
def health():
    print("Request received")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_hour_video"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    print("Request served")
    return jsonify(json_data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
