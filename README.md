
# Airflow FIAP Tech Challenge

## Description

This project is designed to extract meteorological data from the OpenWeather API using Apache Airflow. The data collected spans from January 1, 2022, to the current day. The pipeline processes the data and uploads both the raw (bronze) and transformed (silver) datasets to AWS S3.

## Technologies Used

- Apache Airflow
- OpenWeather API
- AWS S3
- Python

## Setup Instructions

Follow these steps to set up the project locally:

```bash
# Clone the repository
git clone https://github.com/nikolly/airflow-fiap-tech-challenge.git

# Navigate into the project directory
cd airflow-fiap-tech-challenge

# Install the required dependencies
pip install -r requirements.txt

# Config Airflow
https://airflow.apache.org/docs/apache-airflow/stable/start.html
```

## Pipeline Overview

The Airflow pipeline fetches weather data in real-time and processes it as follows:

- **Extract**: Data is collected from the OpenWeather API.
- **Transform**: The data is processed to simplify JSON field access, remove unnecessary fields, and convert the data to `.parquet` format before uploading it to AWS S3.
- **Load**: The raw and processed data are stored in AWS S3.

## Data Storage Structure

The data is stored in AWS S3 in the following structure:

- `bronze/`: Contains the raw data collected from the API.
- `silver/`: Contains the transformed `.parquet` data, which is simplified and optimized for analysis.

## Prerequisites

Before running the pipeline, ensure that the following software is installed and configured on your machine:

1. **Apache Spark 3.1.3**:
   - Download the pre-built package for Hadoop 3.2 from the [official Apache Spark download page](https://archive.apache.org/dist/spark/spark-3.1.3/).
   - Extract the downloaded file and set the `SPARK_HOME` environment variable to the extracted directory:

     ```bash
     export SPARK_HOME=/path/to/spark-3.1.3-bin-hadoop3.2
     ```

2. **Hadoop 3.2**:
   - Download Hadoop from the [official Hadoop page](https://hadoop.apache.org/releases.html), and set the `HADOOP_HOME` environment variable:

     ```bash
     export HADOOP_HOME=/path/to/hadoop
     ```

Make sure both **Spark** and **Hadoop** binaries are included in your system's `PATH`:

```bash
export PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
```

## AWS S3 Configuration for Airflow

To upload data to AWS S3, you need to configure the AWS S3 connection in Airflow. Follow these steps:

1. Open the Airflow UI at `http://localhost:8080`.
2. Go to the **Admin > Connections** section.
3. Click **"+"** to add a new connection.
4. In the **Add Connection** form, enter the following:
   - **Connection Id**: `aws_default`
   - **Connection Type**: `Amazon Web Services`
   - **Login**: Your AWS Access Key ID
   - **Password**: Your AWS Secret Access Key
   - **Extra**:

     ```json
     {
       "region_name": "your_region"
     }
     ```

5. Click **Save** to save the connection.

For more detailed information on setting up Airflow with AWS, [check this tutorial](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html).

## Usage

Before starting Airflow, make sure to export the necessary environment variable for Airflow:

```bash
export AIRFLOW_HOME=/path/to/airflow
```

To start Airflow in standalone mode, use the following command:

```bash
airflow standalone
```

You can monitor the status of your jobs through the Airflow UI at `http://localhost:8080`.

## Future Work

In the next phase, a machine learning model will be integrated into the project to analyze the collected weather data.
