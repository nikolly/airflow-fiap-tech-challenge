import sys
"""
This module defines an Apache Airflow DAG for extracting weather data using the OpenWeather API.
Modules:
    sys: Provides access to some variables used or maintained by the interpreter and to functions that interact strongly with the interpreter.
    airflow.models.DAG: Provides the DAG class to create a Directed Acyclic Graph.
    operators.openweather_operator.OpenWeatherOperator: Custom operator to interact with the OpenWeather API.
    os.path.join: Joins one or more path components intelligently.
    airflow.utils.dates.days_ago: Utility to calculate dates relative to the current date.
DAG:
    OpenWeatherDAG: A DAG that runs daily to extract weather data from the OpenWeather API.
Tasks:
    to: An instance of OpenWeatherOperator that extracts weather data and saves it to a specified file path.
"""
sys.path.append("airflow_pipeline")

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.openweather_operator import OpenWeatherOperator
from pathlib import Path
from os.path import join
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


with DAG(
    dag_id="OpenWeatherDAG_S3",
    start_date=days_ago(701),
    schedule_interval="@daily"
) as dag:
    
    # Configurações do S3
    S3_BUCKET = 'openweather-tc3'  # Substitua pelo nome do seu bucket
    BASE_S3_PATH = "{stage}/"
    
    PARTITION_FOLDER_EXTRACT = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
    
        
    BASE_FOLDER = join(
       str(Path("~/Documents").expanduser()),
       "airflow_alura/datalake/{stage}/openweather/{partition}",
    )

     # Tarefa para extrair dados e salvar localmente
    weather_operator = OpenWeatherOperator(file_path=join(BASE_FOLDER.format(stage='Bronze', partition=PARTITION_FOLDER_EXTRACT), 
                                                          "openweather_{{ ds_nodash }}.json"),
                                           date="{{ data_interval_start.strftime('%Y-%m-%d') }}", 
                                           task_id="extract_openweather")
    
    # Tarefa para fazer upload para o S3
    upload_bronze_task = LocalFilesystemToS3Operator(
        task_id='upload_bronze_to_s3',
        filename=join(BASE_FOLDER.format(stage='Bronze', partition=PARTITION_FOLDER_EXTRACT), "openweather_{{ ds_nodash }}.json"),
        dest_key=BASE_S3_PATH.format(stage='Bronze') + "openweather_{{ ds_nodash }}.json",
        dest_bucket=S3_BUCKET,
        replace=True,
        aws_conn_id='aws_default'
    )
    
    # Tarefa para transformar os dados diretamente do S3
    weather_transform = SparkSubmitOperator(
        task_id="transform_openweather", 
        application="src/spark/transformation.py",
        name="weather_transformation",
        application_args=["--src", BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
        "--dest", BASE_FOLDER.format(stage="Silver", partition=""),
        "--process-date", "{{ ds }}"]
    )
    
    # Tarefa para fazer upload para o S3
    upload_silver_task = LocalFilesystemToS3Operator(
        task_id='upload_silver_to_s3',
        filename=join(BASE_FOLDER.format(stage="Silver", partition=""), "process_date={{ ds }}/openweather_{{ ds }}.parquet"),
        dest_key=BASE_S3_PATH.format(stage='Silver') + "openweather_{{ ds_nodash }}.parquet",
        dest_bucket=S3_BUCKET,
        replace=True,
        aws_conn_id='aws_default'
    )
    
    weather_operator >> upload_bronze_task >> weather_transform >> upload_silver_task
