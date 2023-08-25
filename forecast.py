import requests
import pandas as pd
import json
from urllib.parse import urlencode
import re

def to_snake_case(text: str) -> str:
    """
    Converts a camelCase or PascalCase string to snake_case.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class SurflineWrapper:
    """
    A client for interacting with the Surfline API to fetch forecast data.
    """
    BASE_URL = "https://services.surfline.com/kbyg/spots/forecasts/"

    def __init__(self):
        self.session = requests.Session()

    def fetch_forecast(self, params: dict):
        data = {}
        for attr in ('', 'wave', 'wind', 'tides', 'weather'):
            data[attr] = self._fetch_attr_response(params, attr)

        return self._process_response(data)

    def _fetch_attr_response(self, params: dict, forecast_attr: str) -> dict:
        url = f"{self.BASE_URL}{forecast_attr}?{urlencode(params)}"
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}\n{response.reason}")
        return json.loads(response.text)

    def _process_response(self, data: dict) -> dict:
        # Meta
        forecast = {'meta': {
            'spotId': data['']['spotId'],
            'utcOffset': data['']['utcOffset'],
            'units': data['']['units']
        }, 'surf': [], 'swells': [], 'wind': [], 'tides': [], 'weather': []}

        # Surf and Swells
        for wave_obs in data['wave']['data']['wave']:
            surf_obs = {
                'timestamp': wave_obs['timestamp'],
                'min': wave_obs['surf']['min'],
                'max': wave_obs['surf']['max'],
                'optimalScore': wave_obs['surf']['optimalScore'],
                'humanRelation': wave_obs['surf']['humanRelation'],
                'rawMin': wave_obs['surf']['raw']['min'],
                'rawMax': wave_obs['surf']['raw']['max']
            }

            swell_obs = {
                'timestamp': wave_obs['timestamp'],
                'swells': wave_obs['swells']
            }

            forecast['surf'].append(surf_obs)
            forecast['swells'].append(swell_obs)

        # Wind
        for obs in data['wind']['data']['wind']:
            wind_obs = {x: obs[x] for x in ('timestamp', 'speed', 'direction', 'directionType', 'gust', 'optimalScore')}
            forecast['wind'].append(wind_obs)

        # Tides
        for obs in data['tides']['data']['tides']:
            tides_obs = {x: obs[x] for x in ('timestamp', 'type', 'height')}
            forecast['tides'].append(tides_obs)

        # Weather
        for obs in data['weather']['data']['weather']:
            weather_obs = {x: obs[x] for x in ('timestamp', 'temperature', 'condition', 'pressure')}
            forecast['weather'].append(weather_obs)

        # SunriseSunsetTimes
        forecast['sunriseSunsetTimes'] = {
            x: data['weather']['data']['sunlightTimes'][0][x] for x in ('midnight', 'dawn', 'sunrise', 'sunset', 'dusk')
        }

        return forecast

class Forecast:
    def __init__(self, data: dict):
        self.data = data

        # Meta Attributes
        self.spot_id = self.data['meta']['spotId']
        self.utc_offset = self.data['meta']['utcOffset']
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