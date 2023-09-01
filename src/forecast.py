import requests
import pandas as pd
import json
from urllib.parse import urlencode
from utils import to_snake_case, datetime_serializer
from datetime import datetime


class SurflineWrapper:
    """
    A wrapper for interacting with the Surfline API to fetch forecast data, and process it into a python dictionary.
    The naming convention does not follow Surfline's as this converts to snake_case and processes it differently.
    """
    BASE_URL = "https://services.surfline.com/kbyg/spots/forecasts/"

    def __init__(self) -> None:
        """
        Initialize a new instance of SurflineWrapper.
        """
        # initializes a session to be used for making API requests.
        self.session = requests.Session()

    def fetch_forecast(self, params: dict) -> dict:
        """
        Fetch the forecast data from the Surfline API.

        This function fetches forecast data from the Surfline API based on the provided parameters, and includes
        processing into a more consistent/usable format. This API call cannot use Surfline pro, so is limited to up to
        five days as a parameter.

        :param params: Parameters dictionary to be included in the API request.
        :return: A dictionary containing processed surf forecast data with various categories such as 'meta', 'surf',
        'swell', 'wind', 'tide', 'weather', and 'sunlight_times'.
        """
        response_data = {}
        for attr in ('', 'wave', 'wind', 'tides', 'weather'):
            response_data[attr] = self._fetch_attr_response(params, attr)

        return self._process_responses(response_data, params['spotId'])

    def _fetch_attr_response(self, params: dict, forecast_attr: str) -> dict:
        """
        Fetch a specific forecast attribute response from the Surfline API.

        :param params: Parameters to be included in the API request.
        :param forecast_attr: The attribute to fetch data for.
        :return:
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

        :param response_data: The raw responses data from Surfline's API.
        :param spot_id: The spot id in the request.
        :return: A dictionary containing processed surf forecast data with various categories such as 'meta', 'surf',
        'swell', 'wind', 'tide', 'weather', and 'sunlight_times'.
        """
        # Initialize forecast_data dictionary with meta data filled
        forecast_data = {'meta': {
            'spot_id': spot_id,
            'utc_offset': response_data['']['utcOffset'],
            'units': response_data['']['units']
        }, 'surf': [], 'swell': [], 'wind': [], 'tide': [], 'weather': [], 'forecast': []}

        # Wave (Surf and Swells)
        for wave_obs in response_data['wave']['data']['wave']:
            # convert timestamp to a datetime object
            timestamp = datetime.fromtimestamp(wave_obs['timestamp'])

            # Surf
            forecast_data['surf'].append({
                'timestamp': timestamp,
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
                'timestamp': timestamp,
                **{
                    to_snake_case(key): [wave_obs['swells'][i][key]
                                         for i in range(len(wave_obs['swells']))]
                    for key in wave_obs['swells'][0]
                }
            })

            # Forecast Probability
            forecast_data['forecast'].append({
                'timestamp': timestamp,
                'probability': wave_obs['probability']
            })

        # Wind, Tide and Weather
        for new_key, key, attributes in (
            ('wind', 'wind', ('timestamp', 'speed', 'direction', 'directionType', 'gust', 'optimalScore')),
            ('tide', 'tides', ('timestamp', 'type', 'height')),
            ('weather', 'weather', ('timestamp', 'temperature', 'condition', 'pressure'))
        ):
            for obs in response_data[key]['data'][key]:
                # convert timestamp to a datetime
                timestamp = datetime.fromtimestamp(obs['timestamp'])
                forecast_data[new_key].append({
                    to_snake_case(attr): (obs[attr] if attr != 'timestamp' else timestamp) for attr in attributes
                })

        # Sunlight Times
        forecast_data['sunlight_times'] = [{
            # convert to datetime objects
            key: datetime.fromtimestamp(response_data['weather']['data']['sunlightTimes'][i][key])
            for key in ('midnight', 'dawn', 'sunrise', 'sunset', 'dusk')
        } for i in range(len(response_data['weather']['data']['sunlightTimes']))]

        return forecast_data


class Forecast:
    """
    A class for forecast objects.
    """
    def __init__(self, data: dict):
        """
        Initialize the Forecast object.

        :param data: A dictionary containing forecast data.
        """
        self.data = data

        # Meta Attributes
        self.spot_id = self.data['meta']['spot_id']
        self.utc_offset = self.data['meta']['utc_offset']
        self.timestamps = [entry["timestamp"] for entry in self.data['surf']]

    def __repr__(self) -> str:
        return f"Forecast Object : (spot_id : {self.spot_id}, timestamps : {self.timestamps})"

    def flatten(self) -> list:
        """
        Flatten the forecast data ready to be converted to a dataframe or stored into a database.

        :return: A list of flattened dictionaries.
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

        return flattened_data

    def sunlight_times(self, add_date=True):
        sunlight_times = self.data['sunlight_times']

        if add_date:
            for i, entry in enumerate(self.data['sunlight_times']):
                date = entry["sunrise"].date()
                sunlight_times[i]["date"] = date.isoformat()

        return sunlight_times

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert the forecast data to a DataFrame.

        :return: The DataFrame representation of the forecast data.
        """
        return pd.DataFrame(self.flatten())

    def to_json(self) -> str:
        """
        Convert the forecast data to a JSON string.

        :return: The JSON string representation of the forecast data.
        """
        return json.dumps(self.data, indent=4, default=datetime_serializer)
