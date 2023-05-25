FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install requirements
RUN pip install -r requirements.txt

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
COPY ./ .
