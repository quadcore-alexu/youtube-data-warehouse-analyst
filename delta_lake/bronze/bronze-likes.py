from confluent_kafka import Consumer
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from datetime import datetime
from delta import *
import json
import params


def form_log_record(message):
    return {"timestamp_seconds": int(datetime.timestamp(datetime.now())), "user_id": message["user_id"], "user_country": message["user_country"],
            "user_age": message["user_age"], "video_id": message["video_id"], "channel_id": message["channel_id"], "seconds_offset": message["seconds_offset"]}


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    data_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_seconds", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("video_id", IntegerType(), True),
        StructField("channel_id", IntegerType(), True),
        StructField("seconds_offset", IntegerType(), True)
    ])

    c = Consumer(
        {'bootstrap.servers': params.kafka_listeners, 'group.id': 'delta'})
    c.subscribe(['likes'])

    batch_size = 1000
    message_count = 0
    records = []

    while True:
        msg = c.poll(5.0)
        if msg is None:
            continue
        else:
            message_count += 1
            records.append(form_log_record(
                json.loads(msg.value().decode('utf-8'))))
            if message_count > batch_size:
                print("Likes batch written")
                parsed_df = spark.createDataFrame(records, schema=data_schema)
                parsed_df = parsed_df.withColumn(
                    "timestamp", from_unixtime(parsed_df["timestamp_seconds"]))
                delta_path = "hdfs://namenode:9000/tmp/bronze_likes"
                parsed_df.write.format("delta").mode("append").save(delta_path)
                message_count = 0
                del records
                records = []
