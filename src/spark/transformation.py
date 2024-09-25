from pyspark.sql.functions import col
from os.path import join
from pyspark.sql import SparkSession
import argparse
import os
import shutil
import glob


def get_weather_data(df):
    weather_df = df.select(
        col("date"),
        col("cloud_cover").getItem("afternoon").alias("cloud_cover_afternoon"),
        col("humidity").getItem("afternoon").alias("humidity_afternoon"),
        col("precipitation").getItem("total").alias("precipitation_total"),
        col("temperature").getItem("max").alias("temp_max"),
        col("temperature").getItem("min").alias("temp_min"),
        col("temperature").getItem("afternoon").alias("temp_afternoon"),
        col("temperature").getItem("evening").alias("temp_evening"),
        col("temperature").getItem("morning").alias("temp_morning"),
        col("temperature").getItem("night").alias("temp_night"),
        col("pressure").getItem("afternoon").alias("pressure_afternoon"),
        col("wind").getItem("max").getField("speed").alias("wind_speed_max"),
        col("wind").getItem("max").getField("direction").alias("wind_direction_max")
    )
    
    return weather_df


def export_parquet(df, dest):
    df.coalesce(1).write.mode("overwrite").parquet(dest)
    
    temp_file = glob.glob(os.path.join(dest, "part-*"))[0]
    final_file = os.path.join(dest, f"openweather_{os.path.basename(dest).split('=')[1]}.parquet") 

    shutil.move(temp_file, final_file)
    
    
def weather_transformation(spark, src, dest, process_date):
    df = spark.read.json(src)

    weather_df = get_weather_data(df)
    table_dest = join(dest, f"process_date={process_date}")

    export_parquet(weather_df, table_dest)
    
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Spark Weather Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)

    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("weather_transformation")\
        .getOrCreate()

    weather_transformation(spark, args.src, args.dest, args.process_date)
    