from confluent_kafka import Consumer
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
import time
import params
from threading import Thread
from delta import *
import json


def run_client():
    print("Insert thread: 0")
    builder = pyspark.sql.SparkSession.builder.appName("MyApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("Insert thread: 1")
    data_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("day", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("country", StringType(), True)
    ])
    c = Consumer({ 'bootstrap.servers': params.kafka_listeners, 'group.id': 'delta', 'auto.offset.reset': 'earliest' })
    print("Insert thread: 2")
    c.subscribe([params.topic_name])
    print("Insert thread: 3")
    i = 0
    while i < 10:
        print("Insert thread: 4")
        msg = c.poll(1.0)
        if msg is None:
            continue
        else:
            print('Received message: {0}'.format(msg.value().decode('utf-8')))
            msg_str = str(msg.value(), 'utf-8')
            msg_str = msg_str.replace("'", "\"")
            message = json.loads(msg_str)
            data = { "video_id": message["video_id"], "day": message["day"], "timestamp": message["timestamp"], "country": message["country"] }
            parsed_df = spark.createDataFrame([data], schema=data_schema)
            delta_path = "hdfs://namenode:9000/tmp/bronze_1"
            parsed_df.write.format("delta").mode("append").save(delta_path)
            i += 1
        


if __name__ == '__main__':
    run_client()

    print("Read thread")
    builder = pyspark.sql.SparkSession.builder.appName("MyApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "hdfs://namenode:9000/tmp/bronze_1"
    time.sleep(10)
    print("Start showing")
    while True:
        # Read the Delta table
        bronze_df = spark.read.format("delta").load(bronze_path)
        # Query the table
        bronze_df.show()

        print("Sleeping....")
        time.sleep(5)
