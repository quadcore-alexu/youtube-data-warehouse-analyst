from transformers import pipeline


class SentimentAnalysis:
    def __int__(self):
        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.label_to_score = {"positive": 1, "neutral": 0, "negative": -1}
        self.sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

    def classify(self, strings):
        """
        Performs sentiment classification on the given input.
        Args:
            strings (list): A list of texts to classify.
        Returns:
            A list of sentiment scores calculated based on the sentiment classification.
        Raises:
            ValueError: If the input is not a list.
        """

        # input = ["قاعد بعيده من امبارح", "ههههه", "ايه القرف دا", "جميل جدا"]
        if not isinstance(input, list):
            raise ValueError("Input must be a list.")
        classification = self.sentiment_task(strings)
        scores = [c['score'] * self.label_to_score[c['label']] for c in classification]
        return scores
