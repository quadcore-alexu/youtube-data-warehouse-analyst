FROM openjdk:11

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

RUN pip install pandas
RUN pip install pyspark==3.3.0
RUN pip install delta-spark==2.1.0
RUN pip install transformers
RUN pip install sentencepiece
RUN pip install protobuf==3.20.*
RUN pip install torch==2.0.1
RUN pip install xformers

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy the contents of the pseudoclient folder on the host machine to the /app directory inside the container
COPY ./ .

# Set the default command to run when the container starts, which in this case is to run the driver.py script using the python command
CMD ["python3", "driver.py"]
