#!/bin/bash

# Define the number of requests to send
NUM_REQUESTS=100

# Define the API endpoints
API_ENDPOINTS=(
  "http://172.18.0.4:8000/scylla/video_history?video_id=15"
  "http://172.18.0.4:8000/scylla/channel_history?channel_id=5"
  "http://172.18.0.4:8000/scylla/top_watched_videos?level=last_hour"
  "http://172.18.0.4:8000/scylla/top_watched_videos?level=last_day"
  "http://172.18.0.4:8000/scylla/top_watched_videos?level=last_week"
  "http://172.18.0.4:8000/scylla/top_watched_videos?level=last_month"
  "http://172.18.0.4:8000/scylla/top_watched_videos?level=lalltime"
  "http://172.18.0.4:8000/scylla/top_watched_channels?level=last_hour"
  "http://172.18.0.4:8000/scylla/top_watched_channels?level=last_day"
  "http://172.18.0.4:8000/scylla/top_watched_channels?level=last_week"
  "http://172.18.0.4:8000/scylla/top_watched_channels?level=last_month"
  "http://172.18.0.4:8000/scylla/top_watched_channels?level=alltime"
  "http://172.18.0.4:8000/scylla/top_liked_videos?level=last_hour"
  "http://172.18.0.4:8000/scylla/top_liked_videos?level=last_day"
  "http://172.18.0.4:8000/scylla/top_liked_videos?level=last_week"
  "http://172.18.0.4:8000/scylla/top_liked_videos?level=last_month"
  "http://172.18.0.4:8000/scylla/top_liked_videos?level=alltime"
  "http://172.18.0.4:8000/scylla/top_liked_channels?level=last_hour"
  "http://172.18.0.4:8000/scylla/top_liked_channels?level=last_day"
  "http://172.18.0.4:8000/scylla/top_liked_channels?level=last_week"
  "http://172.18.0.4:8000/scylla/top_liked_channels?level=last_month"
  "http://172.18.0.4:8000/scylla/top_liked_channels?level=alltime"
  "http://172.18.0.4:8000/scylla/interaction?channel_id=5"
  "http://172.18.0.4:8000/scylla/countries?channel_id=5"
  "http://172.18.0.4:8000/scylla/ages?channel_id=5"
  "http://172.18.0.4:8000/scylla/histogram?video_id=15"
)

# Initialize variables
total_time=0

# Loop over each API endpoint
for endpoint in "${API_ENDPOINTS[@]}"; do
  echo "Endpoint: $endpoint"

  # Initialize variables for each endpoint
  endpoint_total_time=0

  # Send requests and calculate response time
  for ((i=1; i<=NUM_REQUESTS; i++)); do
    start_time=$(date +%s%3N)  # Start time in milliseconds
    
    # Make the API request and capture the response
    response=$(curl -s "$endpoint")
    end_time=$(date +%s%3N)  # End time in milliseconds
    response_time=$((end_time - start_time))  # Response time in milliseconds

    endpoint_total_time=$((endpoint_total_time + response_time))
  done

  # Calculate and display the average response time for the current endpoint
  average_time=$((endpoint_total_time / NUM_REQUESTS))
  echo "Average Response Time: $average_time ms"

  # Add the endpoint's average response time to the total time
  total_time=$((total_time + endpoint_total_time))

  echo
done

# Calculate and display the overall average response time
overall_average_time=$((total_time / (NUM_REQUESTS * ${#API_ENDPOINTS[@]})))
echo "Overall Average Response Time: $overall_average_time ms"