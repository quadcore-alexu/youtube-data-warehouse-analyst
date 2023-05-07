FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install PySpark and Delta Lake
RUN pip3 install pyspark==3.1.2
RUN pip3 install delta-spark==2.1.0
RUN pip3 install confluent-kafka
RUN pip install pyspark[sql-kafka]


# Create the jars directory
RUN mkdir -p /usr/local/spark/jars/

# Download Kafka jars
RUN wget https://downloads.apache.org/kafka/2.8.2/kafka_2.12-2.8.2.tgz && \
    tar -xzf kafka_2.12-2.8.2.tgz && \
    cp kafka_2.12-2.8.2/libs/*.jar /usr/local/spark/jars/

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
COPY ./ .

# Set the entrypoint command to run PySpark
CMD ["python3", "script.py"]
