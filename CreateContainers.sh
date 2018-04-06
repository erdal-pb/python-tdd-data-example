#!/bin/bash

# Build the Docker image
docker build -t etl-with-luigi .

# Start the MongoDB container daemon 
# The stock image has a persistent data volume at /data/db
docker pull mongo
docker run --name mongoserver -d mongo

printf "\n \n Loading up the task environment \n ========================= \n\n
Start Task 1 or Task 2 via ./StartTask1.sh or ./StartTask2.sh \n\n"

# Start the app environment and link to the Mongo container
docker run -it --link mongoserver etl-with-luigi bash
