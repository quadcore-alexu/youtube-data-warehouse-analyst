FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install requirements
RUN pip3 install Flask==2.1.2
RUN pip3 install pyspark==3.1.2
RUN pip3 install delta-spark

COPY ./ .

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD [ "python3", "app.py" ]