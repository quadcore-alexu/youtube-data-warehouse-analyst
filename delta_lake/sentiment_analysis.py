from transformers import pipeline
from pyspark.sql import Column


class SentimentAnalysis:
    def __int__(self):
        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.label_to_score = {"positive": 1, "neutral": 0, "negative": -1}
        self.sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

    def classify(self, column):
        """
        Performs sentiment classification on the given input.
        Args:
            column (pyspark.sql.Column): A PySpark DataFrame column containing texts to classify.
        Returns:
            A PySpark DataFrame column of sentiment scores calculated based on the sentiment classification.
        Raises:
            ValueError: If the input is not a PySpark DataFrame column.
        """
        if not isinstance(column, Column):
            raise ValueError("Input must be a PySpark DataFrame column.")

        # Perform sentiment classification on the given column
        classification = self.sentiment_task(column)

        # input = ["قاعد بعيده من امبارح", "ههههه", "ايه القرف دا", "جميل جدا"]

        scores = classification["score"] * self.label_to_score[classification["label"]]
        return scores
