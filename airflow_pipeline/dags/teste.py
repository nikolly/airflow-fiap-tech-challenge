import boto3

s3 = boto3.client('s3')

s3.upload_file('/home/nikolly/Documents/airflow_alura/datalake/Bronze/openweather/extract_date=2024-09-15/openweather_20240915.json', 'teste-nikolly', 'teste')
