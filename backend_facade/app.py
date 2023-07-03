from urllib import request

import pyspark
from delta import *
import json
from flask import Flask, jsonify

app = Flask(__name__)
builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions",
                                                                      "io.delta.sql.DeltaSparkSessionExtension").config(
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


@app.route('/delta/video_history')
def get_video_history():
    video_id = request.args.get("video_id")

    gold_table_last_hour = "hdfs://namenode:9000/tmp/gold_last_hour_video"
    gold_table_last_day = "hdfs://namenode:9000/tmp/gold_last_day_video"
    gold_table_last_week = "hdfs://namenode:9000/tmp/gold_last_week_video"
    gold_table_last_month = "hdfs://namenode:9000/tmp/gold_last_month_video"
    gold_table_all = "hdfs://namenode:9000/tmp/gold_alltime_video"

    last_hour_df = spark.read.format("delta").load(gold_table_last_hour).where((col("video_id") == video_id))
    last_day_df = spark.read.format("delta").load(gold_table_last_day).where((col("video_id") == video_id))
    last_week_df = spark.read.format("delta").load(gold_table_last_week).where((col("video_id") == video_id))
    last_month_df = spark.read.format("delta").load(gold_table_last_month).where((col("video_id") == video_id))
    all_df = spark.read.format("delta").load(gold_table_all).where((col("video_id") == video_id))

    json_last_hour = json.loads((last_hour_df.toJSON().collect())[0])
    json_last_day = json.loads((last_day_df.toJSON().collect())[0])
    json_last_week = json.loads((last_week_df.toJSON().collect())[0])
    json_last_month = json.loads((last_month_df.toJSON().collect())[0])
    json_all = json.loads((all_df.toJSON().collect())[0])

    response = {"last_hour": json_last_hour,
                "last_day": json_last_day,
                "last_week": json_last_week,
                "last_month": json_last_month,
                "all": json_all}

    return jsonify(response)


@app.route('/channel_history')
def get_channel_history():
    channel_id = request.args.get("channel_id")

    gold_table_last_hour = "hdfs://namenode:9000/tmp/gold_last_hour_channel"
    gold_table_last_day = "hdfs://namenode:9000/tmp/gold_last_day_channel"
    gold_table_last_week = "hdfs://namenode:9000/tmp/gold_last_week_channel"
    gold_table_last_month = "hdfs://namenode:9000/tmp/gold_last_month_channel"
    gold_table_all = "hdfs://namenode:9000/tmp/gold_alltime_channel"

    last_hour_df = spark.read.format("delta").load(gold_table_last_hour).where((col("channel_id") == channel_id))
    last_day_df = spark.read.format("delta").load(gold_table_last_day).where((col("channel_id") == channel_id))
    last_week_df = spark.read.format("delta").load(gold_table_last_week).where((col("channel_id") == channel_id))
    last_month_df = spark.read.format("delta").load(gold_table_last_month).where((col("channel_id") == channel_id))
    all_df = spark.read.format("delta").load(gold_table_all).where((col("channel_id") == channel_id))

    json_last_hour = json.loads((last_hour_df.toJSON().collect())[0])
    json_last_day = json.loads((last_day_df.toJSON().collect())[0])
    json_last_week = json.loads((last_week_df.toJSON().collect())[0])
    json_last_month = json.loads((last_month_df.toJSON().collect())[0])
    json_all = json.loads((all_df.toJSON().collect())[0])

    response = {"last_hour": json_last_hour,
                "last_day": json_last_day,
                "last_week": json_last_week,
                "last_month": json_last_month,
                "all": json_all}

    return jsonify(response)


@app.route('/top_watched_videos')
def get_top_watched_videos():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_video"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("views_count"), reverse=True)
    return jsonify(sorted_data)


@app.route('/top_watched_channels')
def get_top_watched_channels():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_channel"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("views_count"), reverse=True)
    return jsonify(sorted_data)

@app.route('/top_liked_videos')
def get_top_liked_videos():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_video"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("views_count"), reverse=True)
    return jsonify(sorted_data)


@app.route('/top_liked_channels')
def get_top_liked_channels():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_channel"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("likes_count"), reverse=True)
    return jsonify(sorted_data)


@app.route('/comments')
def get_video_comments():
    video_id = request.args.get("video_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_comments"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("video_id") == video_id))
    json_comments = json.loads((gold_df.toJSON().collect())[0])
    comments_count = json_comments.get("comments_count")
    positive_count = json_comments.get("positive_count")
    comments_ratio = positive_count / comments_count
    response = {"comments_positive_ratio": comments_ratio}
    return jsonify(response)


@app.route('/interaction')
def get_interaction():
    channel_id = request.args.get("channel_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_interaction"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("channel_id") == channel_id))
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    return jsonify(json_data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
