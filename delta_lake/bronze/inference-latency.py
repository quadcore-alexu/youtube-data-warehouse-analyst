from transformers import pipeline


# from pyspark.sql import Column
# from delta import *
# from pyspark.sql.types import StringType, FloatType, IntegerType
# import pyspark
# from pyspark.sql.functions import lit, array, udf,col,monotonically_increasing_id

class SentimentAnalysis:
    def __init__(self):
        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.label_to_score = {"positive": 1, "neutral": 0, "negative": -1}
        self.sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path, batch_size=8)

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

sentiment_analyzer = SentimentAnalysis()
text = ['Love you sir!!',
        "Please make videos on..Midpoint circle drawing And Bresenham's circle drawing...",
        'I bought both of your courses on Udemy. You are a very talented teacher. Keep producing quality material. When is your Java course coming along? . I am looking forward to it.',
        'Thank you very much, u really got me in the first video about this algorithm without explanation lol',
        'Another great explanation by Abdul sir. Thank you so much.',
        'do I have to take notes or just see these videos which are helpful',
        'I had no idea what was going on in the first one, but this one cleared everything up!',
        "Just one suggestion: if possible can you provide a link to github links to implementation of your videos as well ? I've tried to watch your videos and implement them in Java but every time I mess up and want to cross-check my implementation, there is no reference code. It would help a lot if you can post a links to implementation of your algorithms :) Thank you",
        'Thankkk youuuuu soooo sooo much sir',
        'great job!!!!',
        'Thank you Sir !!',
        'Thank you so much ❤️',
        "Sir you're doing really great job ..GOD may richly bless you and your family ..much love",
        'I love when he is wearing red because the video is updated :D brilliant',
        'Wonderful explanation (straight from the heaven)',
        'Waw  at the first video I got a confusion,but at this video I have got it.  10Q so much.',
        'Thank you Sir',
        'Sir please upload Approximation Algorithms videos also!! @AbdulBari',
        'Asslamualikum my dear honorable sir Can I know where you from',
        'main aapki jitni prashansa karu, kam hai.',
        'I love this man!',
        "I swear you're the savior of every single CS student. God bless you!. ;-;",
        'sir thank you very very much from the bottom of my heart your work helped me alot🙂',
        'Thank you...🙏🙏🙏.. respect from core of my heart...']


def code_block():
    sentiment_analyzer.classify(text)
    import time

    # Your code goes here
    # ...



if __name__ == '__main__':

    # Perform 10 runs and measure the elapsed time
    total_elapsed_time = 0
    import time
    for _ in range(10):
        start_time = time.time()

        # Execute the code block
        code_block()

        end_time = time.time()
        elapsed_time = end_time - start_time
        total_elapsed_time += elapsed_time

    # Calculate the average elapsed time
    avg_elapsed_time = total_elapsed_time / 10

    # Print the result
    print("Average Elapsed Time:", avg_elapsed_time, "seconds")
