# Use a base image from Docker Hub that has Python 3.6 installed
FROM python:3.6

# Install the Python packages listed in requirements.txt using pip
RUN pip install -r requirements.txt

# Copy the contents of the pseudoclient folder on the host machine to the /app directory inside the container
COPY ./ .

# Set the default command to run when the container starts using the python command
CMD ["python3", "app.py"]
