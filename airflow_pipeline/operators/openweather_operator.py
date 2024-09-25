import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator
from hook.openweather_hook import OpenWeatherHook
import json
from pathlib import Path 

class OpenWeatherOperator(BaseOperator):
    """
    OpenWeatherOperator is a custom Airflow operator to fetch weather data from the OpenWeather API and save it to a specified file.
    Attributes:
        template_fields (list): List of template fields that can be templated by Airflow.
        date (str): The date for which the weather data is to be fetched.
        file_path (str): The file path where the fetched weather data will be saved.
    Methods:
        __init__(file_path, date, **kwargs):
            Initializes the OpenWeatherOperator with the specified file path and date.
        create_parent_folder():
            Creates the parent folder for the specified file path if it does not exist.
        execute(context):
            Executes the operator to fetch weather data for the specified date and save it to the specified file path.
    """
    
    template_fields = ["date", "file_path"]
    
    def __init__(self, file_path, date, **kwargs):
        self.date = date
        self.file_path = file_path

        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) 

    def execute(self, context):
        date = self.date

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            json.dump(OpenWeatherHook(date).run(), output_file)
            output_file.write("\n")
