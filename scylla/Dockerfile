# Use a base image from Docker Hub that has Python 3.6 installed
FROM python:3.9

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the contents of the pseudoclient folder on the host machine to the /app directory inside the container
COPY ./ .

# Install the Python packages listed in requirements.txt using pip
RUN pip install -r requirements.txt
RUN pip install transformers
RUN pip install sentencepiece
RUN pip install torch==2.0.1
RUN pip install xformers
RUN pip install pandas
RUN pip install protobuf==3.20.*


# Set the default command to run when the container starts, which in this case is to run the driver.py script using the python command
CMD ["python", "consumer.py"]
