import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator
from hook.openweather_hook import OpenWeatherHook
import json
from pathlib import Path 

class OpenWeatherOperator(BaseOperator):
    """
    A custom Airflow operator to fetch weather data from the OpenWeather API and store it locally.

    This operator leverages the OpenWeatherHook to retrieve data for a specified date and saves the results
    to a JSON file in the specified file path.

    Attributes:
        file_path (str): The file path where the weather data will be saved.
        date (str): The date for which the weather data is being fetched.
    """
    
    template_fields = ["date", "file_path"]
    
    def __init__(self, file_path, date, **kwargs):
        """
        Initializes the OpenWeatherOperator with the file path and date.

        Args:
            file_path (str): The path where the weather data JSON file will be saved.
            date (str): The date for which weather data is being fetched.
            **kwargs: Additional keyword arguments passed to BaseOperator.
        """
        
        self.date = date
        self.file_path = file_path

        super().__init__(**kwargs)

    def create_parent_folder(self):
        """
        Creates the parent directory for the file path if it doesn't already exist.

        This ensures that the directory structure for the output file is created, preventing errors if the folder is missing.
        """
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) 

    def execute(self, context):
        """
        Executes the operator to fetch weather data and save it to the specified file path.

        Args:
            context (dict): Airflow context dictionary that contains runtime information.

        This method:
        1. Creates the parent folder for the file (if needed).
        2. Fetches the weather data using the OpenWeatherHook.
        3. Writes the retrieved data to the specified JSON file.
        """
        date = self.date

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            json.dump(OpenWeatherHook(date).run(), output_file)
            output_file.write("\n")
