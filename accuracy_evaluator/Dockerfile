FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install requirements
RUN pip install pyspark==3.3.0
RUN pip install delta-spark==2.1.0
RUN pip install confluent-kafka
RUN pip install transformers
RUN pip install sentencepiece
RUN pip install protobuf==3.20.*
RUN pip install torch==2.0.1
RUN pip install xformers
RUN pip install pandas

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
COPY ./ .
