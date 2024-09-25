from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime
import json


class OpenWeatherHook(HttpHook):
    """
    A custom Airflow hook to interact with the OpenWeather API.

    This hook connects to the OpenWeather API to retrieve weather data for a specific date.
    It inherits from Airflow's HttpHook to leverage its built-in functionalities for HTTP requests.

    Attributes:
        date (str): The date for which weather data is fetched.
        conn_id (str): The Airflow connection ID for OpenWeather API. Defaults to 'openweather_default'.
    """
    
    
    def __init__(self, date, conn_id=None):
        """
        Initializes the hook with the date and optional connection ID.

        Args:
            date (str): The date for which weather data is fetched, in the format 'YYYY-MM-DDTHH:MM:SS.00Z'.
            conn_id (str, optional): The Airflow connection ID for OpenWeather API. Defaults to 'openweather_default'.
        """
        
        self.date = date
        self.conn_id = conn_id or "openweather_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        """
        Creates the URL required to fetch weather data from the OpenWeather API.

        Returns:
            str: The complete URL for the API request.
        """
        
        date = self.date
        app_id = "b2fc7282e969bdd471e58aaa1a5b34b5"

        url_raw = f"{self.base_url}/day_summary?lat=-16.680882&lon=-49.2532691&appid={app_id}&date={date}&units=metric"

        return url_raw

    def connect_to_endpoint(self, url, session):
        """
        Connects to the given URL endpoint using the provided session and makes an HTTP GET request.

        Args:
            url (str): The API endpoint URL.
            session (requests.Session): A session object used to prepare and send the request.

        Returns:
            requests.Response: The response from the OpenWeather API.
        """
        
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})
    
    def run(self):
        """
        Runs the full process of connecting to the OpenWeather API and retrieving the weather data.

        Returns:
            dict: The parsed JSON response from the OpenWeather API.
        """
        
        session = self.get_conn()
        url_raw = self.create_url()
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()

        return json_response

if __name__ == "__main__":
    """
    Example usage: Runs the hook and prints the response in a pretty-printed JSON format.
    """
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    date = datetime.now().strftime(TIMESTAMP_FORMAT)

    for pg in OpenWeatherHook(date).run():
        print(json.dumps(pg, indent=4, sort_keys=True))
