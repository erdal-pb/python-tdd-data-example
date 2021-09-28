# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /ETL-with-Docker

# Copy the current directory contents into the container at /app
ADD . /ETL-with-Docker

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt
RUN apt-get update
RUN apt-get install -y python3-tk

# Make port 80 available to the world outside this container
EXPOSE 80

# Run app.py when the container launches
#CMD ["python", "task1.py", "MakeOutput", "--local-scheduler"]
