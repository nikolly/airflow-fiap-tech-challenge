from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime
import json


class OpenWeatherHook(HttpHook):
    """
    A custom Airflow hook to interact with the OpenWeather API.
    Attributes:
        date (str): The date for which to fetch weather data.
        conn_id (str, optional): The Airflow connection ID to use. Defaults to "openweather_default".
    Methods:
        create_url():
            Constructs the URL for the OpenWeather API request based on the provided date.
        connect_to_endpoint(url, session):
            Sends a GET request to the specified URL using the provided session and returns the response.
        run():
            Executes the process of creating the URL, connecting to the endpoint, and returning the JSON response.
    """

    def __init__(self, date, conn_id=None):
        self.date = date
        self.conn_id = conn_id or "openweather_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):

        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        date = self.date
        app_id = "b2fc7282e969bdd471e58aaa1a5b34b5"

        url_raw = f"{self.base_url}/day_summary?lat=-16.680882&lon=-49.2532691&appid={app_id}&date={date}&units=metric"

        return url_raw

    def connect_to_endpoint(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()

        return json_response

if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    date = datetime.now().strftime(TIMESTAMP_FORMAT)

    for pg in OpenWeatherHook(date).run():
        print(json.dumps(pg, indent=4, sort_keys=True))
