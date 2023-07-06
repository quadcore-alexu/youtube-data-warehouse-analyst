from threading import Thread
import socket
import time
import params
import json
from random import normalvariate
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, DoubleType, FloatType
from datetime import datetime
from delta import *
from sentiment_analysis import SentimentAnalysis

builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

data_schemas = { 
    'first_views': StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("video_id", IntegerType(), True),
        StructField("channel_id", IntegerType(), True)]),
    'views': StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("video_id", IntegerType(), True),
        StructField("channel_id", IntegerType(), True),
        StructField("seconds_offset", IntegerType(), True)]),
    'likes': StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("video_id", IntegerType(), True),
        StructField("channel_id", IntegerType(), True),
        StructField("seconds_offset", IntegerType(), True)]),
    'subscribes': StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("channel_id", IntegerType(), True)]),
    'comments': StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("video_id", IntegerType(), True),
        StructField("channel_id", IntegerType(), True),
        StructField("comment", StringType(), True)])
    }

def form_log_record_first_views(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"], "user_country": message["user_country"],
            "user_age": message["user_age"], "video_id": message["video_id"], "channel_id": message["channel_id"]}

def form_log_record_views(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"],
            "user_country": message["user_country"], "user_age": message["user_age"], "video_id": message["video_id"],
            "channel_id": message["channel_id"], "seconds_offset": message["seconds_offset"]}

def form_log_record_likes(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"], "user_country": message["user_country"],
            "user_age": message["user_age"], "video_id": message["video_id"], "channel_id": message["channel_id"], "seconds_offset": message["seconds_offset"]}

def form_log_record_subscribes(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"], "user_country": message["user_country"],
            "user_age": message["user_age"], "channel_id": message["channel_id"]}

def form_log_record_comments(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"], "user_country": message["user_country"],
            "user_age": message["user_age"], "video_id": message["video_id"], "channel_id": message["channel_id"], "comment": message["comment"]}

def form_log_record(topic, message):
    if topic == 'first_views':
        return form_log_record_first_views(message)
    elif topic == 'views':
        return form_log_record_views(message)
    elif topic == 'likes':
        return form_log_record_likes(message)
    elif topic == 'subscribes':
        return form_log_record_subscribes(message)
    elif topic == 'commments':
        return form_log_record_comments(message)

def write_batch_standard(topic, records):
    data_schema = data_schemas[topic]
    parsed_df = spark.createDataFrame(records, schema=data_schema)
    parsed_df = parsed_df.withColumn(
        "timestamp", from_unixtime(parsed_df["timestamp_seconds"]))
    delta_path = f"hdfs://namenode:9000/tmp/bronze_{topic}"
    parsed_df.write.format("delta").mode("append").save(delta_path)
    del records
    records = []

def write_batch_comments(topic, records, sentiment_analyzer):
    data_schema = data_schemas[topic]
    parsed_df = spark.createDataFrame(records, schema=data_schema).withColumn("id", monotonically_increasing_id())
    classification = sentiment_analyzer.classify(parsed_df.select("comment").toPandas()["comment"].tolist())
    classification_df = spark.createDataFrame(classification, schema=IntegerType()).toDF("comment_score").withColumn("id",
                                                                                        monotonically_increasing_id())
    parsed_df = parsed_df.join(classification_df, on="id", how="inner").drop("id").drop("comment").withColumn("timestamp", from_unixtime(parsed_df["timestamp_seconds"]))
    delta_path = "hdfs://namenode:9000/tmp/bronze_comments"
    parsed_df.write.format("delta").mode("append").save(delta_path)
    del records
    records = []


def write_batch(topic, records, sentiment_analyzer):
    if topic == 'comments':
        return write_batch_comments(topic, records, sentiment_analyzer)
    else:
        return write_batch_standard(topic, records)
    

def normal_int(low, high):
    mean = (high + low) / 2
    stddev = (high - low) / 6
    while True:
        num = int(normalvariate(mean, stddev))
        if low <= num < high:
            return num


def normal_float(low, high):
    mean = (high + low) / 2
    stddev = (high - low) / 6
    while True:
        num = normalvariate(mean, stddev)
        if low <= num < high:
            return num


def normal_choice(lst):
    mean = (len(lst) - 1) / 2
    stddev = len(lst) / 6
    while True:
        index = int(normalvariate(mean, stddev))
        if 0 <= index < len(lst):
            return lst[index]


def gen_message(schema):
    args = {}
    for k, v in schema.items():
        if v['type'] == 'int-range':
            args[k] = normal_int(v['low'], v['high'])
        elif v['type'] == 'float-range':
            args[k] = normal_float(v['low'], v['high'])
        elif v['type'] == 'cat':
            args[k] = normal_choice(v['values'])
        elif v['type'] == 'object':
            args[k] = gen_message(v['schema'])
        elif v['type'] == 'function':
            args[k] = v['function'](args)
        else:
            raise RuntimeError(f'{k} field type not specified or not supported')
    return args


def start_action(args):
    for i in range(args['number_of_batches']):
        insert_into_bronze_table(args['topic'], args['schema'])
        print(f"batch {topic} #{i+1} is inserted")

def insert_into_bronze_table(topic, schema):
    batch_size = 1000
    records = []
    if topic == 'comments':
        sentiment_analyzer = SentimentAnalysis()
    for i in range(batch_size):
        records.append(form_log_record(topic, json.dumps(gen_message(schema))))
    write_batch(topic, records, sentiment_analyzer)
    print(f"{topic} batch written")


def run_client():
    for action in params.actions:
        thread = Thread(target=start_action, args=(action,))
        thread.start()
