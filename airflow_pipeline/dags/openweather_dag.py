import sys
# Adds the custom pipeline directory to the system path so custom modules can be imported
sys.path.append("airflow_pipeline")

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.openweather_operator import OpenWeatherOperator
from pathlib import Path
from os.path import join
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# Define the Airflow DAG (Directed Acyclic Graph) that orchestrates the tasks

with DAG(
    dag_id="OpenWeatherDAG_S3", # Unique identifier for the DAG
    start_date=days_ago(1000),  # Start date for the DAG (1000 days ago) 2022-01-01
    schedule_interval="@daily"  # The interval at which the DAG will be triggered (daily)
) as dag:
    
    # S3 configurations
    S3_BUCKET = 'openweather-tc3'  # S3 bucket where the data will be uploaded
    BASE_S3_PATH = "{stage}/"      # Base path in S3 where the data will be stored
    
    # Partitioning the data folder by the date of extraction
    PARTITION_FOLDER_EXTRACT = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
    
    # Base folder for saving the data locally before uploading to S3
    BASE_FOLDER = join(
       str(Path("~/Documents").expanduser()),
       "airflow_alura/datalake/{stage}/openweather/{partition}",
    )

    # Task: Extract weather data from the OpenWeather API and save it locally
    weather_operator = OpenWeatherOperator(file_path=join(BASE_FOLDER.format(stage='Bronze', partition=PARTITION_FOLDER_EXTRACT), 
                                                          "openweather_{{ ds_nodash }}.json"),
                                           date="{{ data_interval_start.strftime('%Y-%m-%d') }}", 
                                           task_id="extract_openweather")
    
    # Task: Upload the extracted (raw/bronze) data to S3
    upload_bronze_task = LocalFilesystemToS3Operator(
        task_id='upload_bronze_to_s3',
        filename=join(BASE_FOLDER.format(stage='Bronze', partition=PARTITION_FOLDER_EXTRACT), "openweather_{{ ds_nodash }}.json"),
        dest_key=BASE_S3_PATH.format(stage='Bronze') + "openweather_{{ ds_nodash }}.json",
        dest_bucket=S3_BUCKET,
        replace=True,
        aws_conn_id='aws_default'
    )
    
    # Task: Run a Spark job to transform the raw data into the silver stage
    weather_transform = SparkSubmitOperator(
        task_id="transform_openweather", 
        application="src/spark/transformation.py",
        name="weather_transformation",
        application_args=["--src", BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
        "--dest", BASE_FOLDER.format(stage="Silver", partition=""),
        "--process-date", "{{ ds }}"]
    )
    
    # Task: Upload the transformed (silver) data to S3
    upload_silver_task = LocalFilesystemToS3Operator(
        task_id='upload_silver_to_s3',
        filename=join(BASE_FOLDER.format(stage="Silver", partition=""), "process_date={{ ds }}/openweather_{{ ds }}.parquet"),
        dest_key=BASE_S3_PATH.format(stage='Silver') + "openweather_{{ ds_nodash }}.parquet",
        dest_bucket=S3_BUCKET,
        replace=True,
        aws_conn_id='aws_default'
    )
    
    # Defining the task dependencies (order of execution)
    weather_operator >> upload_bronze_task >> weather_transform >> upload_silver_task
