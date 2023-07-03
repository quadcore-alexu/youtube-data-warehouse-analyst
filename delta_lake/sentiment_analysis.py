from transformers import pipeline
from pyspark.sql import Column
from delta import *
from pyspark.sql.types import StringType, FloatType
import pyspark
from pyspark.sql.functions import lit, array, udf,col,monotonically_increasing_id

class SentimentAnalysis:
    def __init__(self):
        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.label_to_score = {"positive": 1, "neutral": 0, "negative": -1}
        self.sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

    def classify(self, text):
        """
        Performs sentiment classification on the given input.
        Args:
            strings (list): A list of texts to classify.
        Returns:
            A list of sentiment scores calculated based on the sentiment classification.
        Raises:
            ValueError: If the input is not a list.
        """


        if not isinstance(text, list):
            raise ValueError("Input must be a list.")

        # Perform sentiment classification on the given column
        classification = self.sentiment_task(text)

        # input = ["قاعد بعيده من امبارح", "ههههه", "ايه القرف دا", "جميل جدا"]

        scores = [ (c["score"] * self.label_to_score[c["label"]]) for c in classification]
        return scores


if __name__ == '__main__':
    # Sample data
    builder = pyspark.sql.SparkSession.builder.appName("DeltaApp").config("spark.sql.extensions",
                                                                          "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    data = [("perfect"), ("too bad"), ("not bad")]

    # Create a DataFrame with a single column named "text"
    df = spark.createDataFrame(data, schema=StringType()).toDF("text")
    sent = SentimentAnalysis()

    parsed_df = spark.createDataFrame(data, schema=StringType()).toDF("text").withColumn("id", monotonically_increasing_id())
    classification = sent.classify(parsed_df.select("text").toPandas()["text"].tolist())
    classification_df = spark.createDataFrame(classification, schema=FloatType()).toDF("comment_score").withColumn("id",
                                                                                                                   monotonically_increasing_id())
    parsed_df = parsed_df.join(classification_df, on="id", how="inner").drop("id").drop("text")

    parsed_df.show()
