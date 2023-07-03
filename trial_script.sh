#!/bin/bash

# Define the number of requests to send
NUM_REQUESTS=100

# Define the API endpoint
API_ENDPOINT="http://localhost:5000"

# Initialize variables
total_time=0

# Send requests and calculate response time
for ((i=1; i<=NUM_REQUESTS; i++)); do
  start_time=$(date +%s%3N)  # Start time in milliseconds
  
  # Make the API request and capture the response
  response=$(curl -s "$API_ENDPOINT")
  end_time=$(date +%s%3N)  # End time in milliseconds
  response_time=$((end_time - start_time))  # Response time in milliseconds

  echo "Request $i: $response_time ms"

  total_time=$((total_time + response_time))
done

# Calculate and display the average response time
average_time=$((total_time / NUM_REQUESTS))
echo "Average Response Time: $average_time ms"

