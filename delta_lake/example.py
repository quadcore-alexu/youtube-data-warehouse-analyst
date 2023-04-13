from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").appName("Creating Delta Lake Table").getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True), StructField("age", IntegerType(), True), StructField("gender", StringType(), True)])

data = [(1, "John", 28, "M"), (2, "Jane", 35, "F"), (3, "Jim", 42, "M")]
df = spark.createDataFrame(data, schema)
df.write.format("delta").save("/myapp/data/example-table")

rdf = spark.read.format("delta").load("/myapp/data/example-table")

rdf.show()

