FROM python:3.12
# Obtaining the Python image to run the script

FROM apache/airflow:2.10.0
# Obtaining the Airflow image to automate the orchestration

USER airflow

# Copying the project to a directory called app (or another name) inside the container
COPY . /app

# Set the working directory to /app
WORKDIR /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt
