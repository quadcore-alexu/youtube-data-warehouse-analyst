from confluent_kafka import Consumer
import sys
import time
import params
import pyspark
from delta import *


def run_client():
    c = Consumer({
        'bootstrap.servers': params.kafka_listeners,
        'group.id': 'delta-group'})

    c.subscribe([params.topic_name])
    i = 0
    while i < 10:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Error while consuming message: {0}'.format(msg.error()))

        else:
            print('Received message: {0}'.format(msg.value().decode('utf-8')))
            i += 1


if __name__ == '__main__':
    run_client()

    builder = pyspark.sql.SparkSession.builder.appName("MyApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(
        "hdfs://namenode:9000/tmp/delta-table")

    df = spark.read.format("delta").load("hdfs://namenode:9000/tmp/delta-table")
    df.show()

