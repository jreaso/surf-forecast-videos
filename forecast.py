import requests
import pandas as pd
import json
from urllib.parse import urlencode
from utils import to_snake_case

# Note: with current handling of `sunlight_times`, if the `days` parameter is anything other than 1 (for multi day
# forecasts), the `sunlight_times` will just refer to the first days sunlight times.


class SurflineWrapper:
    """
    A wrapper for interacting with the Surfline API to fetch forecast data.
    """
    BASE_URL = "https://services.surfline.com/kbyg/spots/forecasts/"

    def __init__(self) -> None:
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

        return self._process_responses(response_data, params['spotId'])

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

    @staticmethod
    def _process_responses(response_data: dict, spot_id: str) -> dict:
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
            'spot_id': spot_id,
            'utc_offset': response_data['']['utcOffset'],
            'units': response_data['']['units']
        }, 'surf': [], 'swell': [], 'wind': [], 'tide': [], 'weather': [], 'forecast': []}

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
                'timestamp': wave_obs['timestamp'],
                **{
                    to_snake_case(key): [wave_obs['swells'][i][key]
                                         for i in range(len(wave_obs['swells']))]
                    for key in wave_obs['swells'][0]
                }
            })

            # Forecast Probability
            forecast_data['forecast'].append({
                'timestamp': wave_obs['timestamp'],
                'probability': wave_obs['probability']
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
    """
    A class for forecast objects.
    """
    def __init__(self, data: dict):
        """
        Initialize the Forecast object.

        Parameters:
            data (dict): A dictionary containing forecast data.

        Attributes:
            data (dict): The input forecast data.
        """
        self.data = data

        # Meta Attributes
        self.spot_id = self.data['meta']['spot_id']
        self.utc_offset = self.data['meta']['utc_offset']
        self.timestamps = [entry["timestamp"] for entry in self.data['surf']]

    def __repr__(self):
        return f"Forecast Object : (spot_id : {self.spot_id}, timestamps : {self.timestamps})"

    def flatten(self) -> list:
        """
        Flatten the forecast data ready to be converted to a dataframe or stored into a database.

        Returns:
            list: A list of flattened dictionaries.
        """
        # Initialize a list of dictionaries with timestamps and meta keys
        flattened_data = [{'spot_id': self.spot_id,
                           'timestamp': timestamp,
                           'utc_offset': self.utc_offset} for timestamp in self.timestamps]

        for i, d in enumerate(flattened_data):
            for attr in ('surf', 'swell', 'wind', 'forecast', 'tide', 'weather'):
                if attr in self.data:
                    # check timestamps match before merging
                    if self.data[attr][i]["timestamp"] == d["timestamp"]:
                        for key, value in self.data[attr][i].items():
                            if key != "timestamp":
                                d[f"{attr}_{to_snake_case(key)}"] = value
                else:
                    raise ValueError(f"No {attr} attribute in forecast data")

        # add an extra key `is_light` in place of `sunlight_times`
        sunrise = self.data['sunlight_times']['sunrise']
        sunset = self.data['sunlight_times']['sunset']

        for d in flattened_data:
            d['is_light'] = 1 if sunrise <= d['timestamp'] <= sunset else 0

        return flattened_data

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert the forecast data to a DataFrame.

        Returns:
            pd.DataFrame: The DataFrame representation of the forecast data.
        """
        df = pd.DataFrame(self.flatten())

        # convert the timestamp to a pandas datetime object
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

        return df

    def to_json(self) -> str:
        """
        Convert the forecast data to a JSON string.

        Returns:
            str: The JSON string representation of the forecast data.
        """
        return json.dumps(self.data, indent=4)
