# Use an official Python image as the base image
FROM python:3.8-slim-bullseye

# Set the working directory inside the container
WORKDIR /app

# Install Java (required for PySpark)
#RUN apt-get install -y default-jre

RUN apt-get update \
    && apt-get install -y openjdk-11-jdk \
    && apt-get clean

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install pre-commit hooks
# COPY .pre-commit-config.yaml .pre-commit-config.yaml
# RUN pre-commit install --install-hooks

# Copy the project files into the container
COPY . .

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3
# ENV SPARK_HOME=/usr/local
# ENV PATH=$PATH:${SPARK_HOME}/bin
#ENV PYTHONPATH=/app/dataset_duplicate_checker

RUN echo $PYTHONPATH

# Expose necessary ports (e.g., for Spark Web UI)
EXPOSE 4040

# Default command to run pytest tests
CMD ["spark-submit", "examples/example_usage.py"]
#CMD ["pytest", "tests/test_duplicate_checker.py"]
