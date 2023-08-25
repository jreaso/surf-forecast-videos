import requests
import pandas as pd
import json
from urllib.parse import urlencode
from utils import to_snake_case

class SurflineWrapper:
    """
    A wrapper for interacting with the Surfline API to fetch forecast data.
    """
    BASE_URL = "https://services.surfline.com/kbyg/spots/forecasts/"

    def __init__(self):
        """
        Initialize a new instance of SurflineWrapper.

        This constructor initializes a session to be used for making API requests.
        """
        self.session = requests.Session()

    def fetch_forecast(self, params: dict):
        """
        Fetch the forecast data from the Surfline API.

        This function fetches forecast data from the Surfline API based on the provided parameters, and includes
        processing into a more consistent/usable format.

        Args:
            params (dict): Parameters to be included in the API request.

        Returns:
            dict: A dictionary containing processed surf forecast data with various categories
                  such as 'meta', 'surf', 'swell', 'wind', 'tide', 'weather', and 'sunlight_times'.
        """
        response_data = {}
        for attr in ('', 'wave', 'wind', 'tides', 'weather'):
            response_data[attr] = self._fetch_attr_response(params, attr)

        return self._process_response(response_data)

    def _fetch_attr_response(self, params: dict, forecast_attr: str) -> dict:
        """
        Fetch a specific forecast attribute response from the Surfline API.

        Args:
            params (dict): Parameters to be included in the API request.
            forecast_attr (str): The attribute to fetch data for.

        Returns:
            dict: The response data from the specified forecast attribute.
        """
        url = f"{self.BASE_URL}{forecast_attr}?{urlencode(params)}"
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}\n{response.reason}")
        return json.loads(response.text)

    def _process_response(self, response_data: dict) -> dict:
        """
        Process and refactor the response data from Surfline's API.

        This function takes the responses data from Surfline's API and refactors the structure to flatten it out and
        change naming conventions to be snake case as well as changing particular names for consistency.

        Args:
            response_data (dict): The raw responses data from Surfline's API.

        Returns:
            dict: A dictionary containing processed surf forecast data with various categories
                  such as 'meta', 'surf', 'swell', 'wind', 'tide', 'weather', and 'sunlight_times'.
        """
        # Initialize forecast_data dictionary with meta data filled
        forecast_data = {'meta': {
            'spot_id': response_data['']['spotId'],
            'utc_offset': response_data['']['utcOffset'],
            'units': response_data['']['units']
        }, 'surf': [], 'swells': [], 'wind': [], 'tide': [], 'weather': []}

        # Wave (Surf and Swells)
        for wave_obs in response_data['wave']['data']['wave']:
            # Surf
            forecast_data['surf'].append({
                'timestamp': wave_obs['timestamp'],
                **{
                    to_snake_case(key): wave_obs['surf'][key]
                    for key in ('min', 'max', 'optimalScore', 'humanRelation')
                },
                **{
                    f"raw_{key}": wave_obs['surf']['raw'][key]
                    for key in ('min', 'max')
                }
            })

            # Swell - flatten the swells into lists
            forecast_data['swell'].append({
                'timestamp': [entry['timestamp'] for entry in forecast_data['swells']],
                **{
                    to_snake_case(key): [[entry['swells'][i][key]
                                          for i in range(len(forecast_data['swells'][0]['swells']))]
                                         for entry in forecast_data['swells']]
                    for key in forecast_data['swells'][0]['swells'][0]
                }
            })

        # Wind, Tide and Weather
        for new_key, key, attributes in (
            ('wind', 'wind', ('timestamp', 'speed', 'direction', 'directionType', 'gust', 'optimalScore')),
            ('tide', 'tides', ('timestamp', 'type', 'height')),
            ('weather', 'weather', ('timestamp', 'temperature', 'condition', 'pressure'))
        ):
            for obs in response_data[key]['data'][key]:
                forecast_data[new_key].append({
                    to_snake_case(attr): obs[attr] for attr in attributes
                })

        # Sunlight Times
        forecast_data['sunlight_times'] = {
            key: response_data['weather']['data']['sunlightTimes'][0][key]
            for key in ('midnight', 'dawn', 'sunrise', 'sunset', 'dusk')
        }

        return forecast_data

class Forecast:
    def __init__(self, data: dict):
        self.data = data

        # Meta Attributes
        self.spot_id = self.data['meta']['spot_id']
        self.utc_offset = self.data['meta']['utc_offset']
        self.timestamps = [entry["timestamp"] for entry in self.data['surf']]


    def flatten(self) -> list:
        # Initialize a list of dictionaries with timestamps and meta keys
        flattened_data = [{'spot_id': self.spot_id,
                           'timestamp': timestamp,
                           'utc_offset': self.utc_offset} for timestamp in self.timestamps]

        for attr in ('surf', 'wind', 'tides', 'weather'):
            pass
#            {f"{attr}_{to_snake_case(key)}" if key != "timestamp" else key:
#                 [entry[key] for entry in self.forecast[attr]] for key in self.forecast[attr][0]}

        self.flat_data = flattened_data
        return self.flat_data

    def to_dataframe(self, attr=None) -> pd.DataFrame:
        return pd.DataFrame(self.flatten())

    def to_json(self) -> str:
        return json.dumps(self.data, indent=4)

class DBManager:
    pass