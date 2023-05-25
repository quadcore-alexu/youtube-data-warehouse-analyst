FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install PySpark and Delta Lake
RUN pip3 install pyspark==3.1.2
RUN pip3 install delta-spark==2.1.0
RUN pip3 install confluent-kafka

RUN pip3 install transformers
RUN pip3 install sentencepiece
RUN pip3 install protobuf==3.20.*

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
COPY ./ .
