# Use a base image from Docker Hub that has Python 3.6 installed
FROM python:3.6

# Copy the contents of the pseudoclient folder on the host machine to the /app directory inside the container
COPY ./ .

# Install the Python packages listed in requirements.txt using pip
RUN pip3 install Flask==2.1.2
RUN pip3 install cassandra-driver==3.25.0

# Set the default command to run when the container starts, which in this case is to run the driver.py script using the python command
CMD ["python3", "app.py"]
