import pyspark
from delta import *
from pyspark.sql.functions import *
import json
import pyspark.sql.functions as fn
from pyspark.sql.types import *
from flask import Flask, jsonify, request

app = Flask(__name__)
builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions",
                                                                      "io.delta.sql.DeltaSparkSessionExtension").config(
    "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


@app.route('/')
def health():
    print("Request received")
    print("Request served")
    return jsonify({'status': 'running'})


@app.route('/delta/count')
def get_records_count():

    bronze_comments_count = spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_comments").count()
    bronze_first_views_count = spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_first_views").count()
    bronze_likes_count = spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_likes").count()
    bronze_subscribers_count = spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_subscribes").count()
    bronze_view_actions_count = spark.read.format("delta").load("hdfs://namenode:9000/tmp/bronze_view_actions").count()

    response = {"count": bronze_comments_count + bronze_first_views_count + bronze_likes_count + bronze_subscribers_count + bronze_view_actions_count}
    return jsonify(response)


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


@app.route('/delta/channel_history')
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


@app.route('/delta/top_watched_videos')
def get_top_watched_videos():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_video"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("views_count", 0), reverse=True)
    if len(sorted_data > 10):
        return jsonify(sorted_data[:10])
    else:
        return jsonify(sorted_data)


@app.route('/delta/top_watched_channels')
def get_top_watched_channels():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_channel"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("views_count", 0), reverse=True)
    if len(sorted_data > 10):
        return jsonify(sorted_data[:10])
    else:
        return jsonify(sorted_data)

@app.route('/delta/top_liked_videos')
def get_top_liked_videos():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_video"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("likes_count", 0), reverse=True)
    if len(sorted_data > 10):
        return jsonify(sorted_data[:10])
    else:
        return jsonify(sorted_data)


@app.route('/delta/top_liked_channels')
def get_top_liked_channels():
    level = request.args.get("level")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_last_" + level + "_channel"
    gold_df = spark.read.format("delta").load(gold_table_path)
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    sorted_data = sorted(json_data, key=lambda x : x.get("likes_count", 0), reverse=True)
    if len(sorted_data > 10):
        return jsonify(sorted_data[:10])
    else:
        return jsonify(sorted_data)


@app.route('/delta/comments')
def get_video_comments():
    video_id = request.args.get("video_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_comments"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("video_id") == video_id))
    json_comments = json.loads((gold_df.toJSON().collect())[0])
    comments_total_count = json_comments.get("comments_count")
    comments_positive_count = json_comments.get("positive_count")
    response = {"comments_positive_count": comments_positive_count, "comments_total_count": comments_total_count}
    return jsonify(response)


@app.route('/delta/interaction')
def get_interaction():
    channel_id = request.args.get("channel_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_interaction"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("channel_id") == channel_id))
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    return jsonify(json_data)


@app.route('/delta/countries')
def get_countries_dist():
    channel_id = request.args.get("channel_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_countries"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("channel_id") == channel_id))

    #gold_df = gold_df.crossJoin(gold_df.groupby("channel_id", "country").agg(fn.sum('views_count').alias('sum_views_count'), fn.sum('likes_count').alias('sum_likes_count'), fn.sum('minutes_count').alias('sum_minutes_count')))
    #gold_df = gold_df.select('channel_id','country', (fn.col('views_count') / fn.col('sum_views_count')).alias('views_percentage'), (fn.col('likes_count') / fn.col('sum_likes_count')).alias('likes_percentage'), (fn.col('minutes_count') / fn.col('sum_minutes_count')).alias('minutes_percentage'))

    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    return jsonify(json_data)


@app.route('/delta/ages')
def get_ages_dist():
    channel_id = request.args.get("channel_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_ages"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("channel_id") == channel_id))

    #gold_df = gold_df.crossJoin(gold_df.groupby("channel_id", "age").agg(fn.sum('views_count').alias('sum_views_count'), fn.sum('likes_count').alias('sum_likes_count'), fn.sum('minutes_count').alias('sum_minutes_count')))
    #gold_df = gold_df.select('channel_id','age', (fn.col('views_count') / fn.col('sum_views_count')).alias('views_percentage'), (fn.col('likes_count') / fn.col('sum_likes_count')).alias('likes_percentage'), (fn.col('minutes_count') / fn.col('sum_minutes_count')).alias('minutes_percentage'))

    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    return jsonify(json_data)


@app.route('/delta/histogram')
def get_video_histogram():
    video_id = request.args.get("video_id")
    gold_table_path = "hdfs://namenode:9000/tmp/gold_video_minute"
    gold_df = spark.read.format("delta").load(gold_table_path).where((col("video_id") == video_id)).orderBy("minutes_offset").select("views_count", "likes_count")
    json_string = gold_df.toJSON().collect()
    json_data = [json.loads(json_str) for json_str in json_string]
    return jsonify(json_data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
