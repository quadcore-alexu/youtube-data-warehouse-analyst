from transformers import pipeline

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

        scores = [self.label_to_score[c["label"]] for c in classification]
        return scores
