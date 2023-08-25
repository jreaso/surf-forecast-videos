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

class Forecast:
    """
    Represents a forecast and provides methods to process and convert the forecast data.
    """
    def __init__(self, params):
        self.params = params
        self._get_forecast()

    def __str__(self):
        return f"Forecast Object\n" + "\n".join([f"{key}: {value}" for key, value in self.params.items()])

    def _get_forecast(self) -> None:
        """
        Uses Surfline API to fetch various forecast json files
        """
        self.api_client = SurflineAPIClient(self.params)

        data = {}
        for forecast_type in ('', 'wave', 'wind', 'tides', 'weather'):
            data[forecast_type] = self.api_client.fetch(forecast_type)

        self._process_forecast(data)

    def _process_forecast(self, data: dict) -> None:
        """
        Uses Surfline API to fetch various forecast json files
        """
        # Meta
        self.forecast = {'meta': {
            'spotId': self.params['spotId'],
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

            self.forecast['surf'].append(surf_obs)
            self.forecast['swells'].append(swell_obs)

        # Wind
        for obs in data['wind']['data']['wind']:
            wind_obs = {x: obs[x] for x in ('timestamp', 'speed', 'direction', 'directionType', 'gust', 'optimalScore')}
            self.forecast['wind'].append(wind_obs)

        # Tides
        for obs in data['tides']['data']['tides']:
            tides_obs = {x: obs[x] for x in ('timestamp', 'type', 'height')}
            self.forecast['tides'].append(tides_obs)

        # Weather
        for obs in data['weather']['data']['weather']:
            weather_obs = {x: obs[x] for x in ('timestamp', 'temperature', 'condition', 'pressure')}
            self.forecast['weather'].append(weather_obs)

        # SunriseSunsetTimes
        self.forecast['sunriseSunsetTimes'] = {
            x: data['weather']['data']['sunlightTimes'][0][x] for x in ('midnight', 'dawn', 'sunrise', 'sunset', 'dusk')
        }

    def to_json(self) -> str:
        """
        Returns the forecast as a JSON string.
        """
        self.json = json.dumps(self.forecast, indent=4)
        return self.json

    def to_dataframe(self, unwrap_swells: bool = False) -> pd.DataFrame:
        """
        Returns the forecast as a pandas DataFrame.

        Args:
            unwrap_swells (bool, optional): Whether to unwrap swells into separate columns. Default is False.

        Returns:
            pd.DataFrame: The forecast data as a pandas DataFrame.
        """
        # Meta
        meta = {to_snake_case(key): self.forecast['meta'][key] for key in ("spotId", "utcOffset")}

        # Surf, Wind, Tide, Weather
        attr_dfs = []
        for attr in ('surf', 'wind', 'tides', 'weather'):
            attr_dfs.append(pd.DataFrame({
                **{f"{attr}_{to_snake_case(key)}" if key != "timestamp" else key:
                       [entry[key] for entry in self.forecast[attr]] for key in self.forecast[attr][0]}
            }))

        # merge the attribute dataframes on timestamp
        merged_attr_df = attr_dfs[0]
        for df in attr_dfs[1:]:
            merged_attr_df = pd.merge(merged_attr_df, df, on="timestamp", how="outer")

        # Swell
        # option for if the dataframe should unwrap the swells into their own columns or into a list
        if unwrap_swells:
            swells_df = pd.DataFrame({
                **{'timestamp': [entry['timestamp'] for entry in self.forecast['swells']]},
                **{f"swell_{i + 1}_{to_snake_case(key)}": [entry['swells'][i][key] for entry in self.forecast['swells']]
                   for i in range(len(self.forecast['swells'][0]['swells']))
                   for key in self.forecast['swells'][0]['swells'][0]}
            })
        else:
            swells_df = pd.DataFrame({
                **{'timestamp': [entry['timestamp'] for entry in self.forecast['swells']]},
                **{f"swell_{to_snake_case(key)}":
                       [[entry['swells'][i][key] for i in range(len(self.forecast['swells'][0]['swells']))]
                        for entry in self.forecast['swells']]
                   for key in self.forecast['swells'][0]['swells'][0]}
            })

        # merge the swells_df with merged_df_attr
        df = pd.merge(merged_attr_df, swells_df, on="timestamp", how="inner")

        # add the meta columns to every row
        for key, value in meta.items():
            df[key] = value

        # convert the timestamp to a pandas datetime object
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

        # add whether the surf spot is light
        sunrise = pd.to_datetime(self.forecast['sunriseSunsetTimes']['sunrise'], unit='s', utc=True)
        sunset = pd.to_datetime(self.forecast['sunriseSunsetTimes']['sunset'], unit='s', utc=True)

        df['is_light'] = df.apply(lambda row: 1 if sunrise <= row['timestamp'] <= sunset else 0, axis=1)

        self.df = df
        return self.df

class SurflineAPIClient:
    """
    A client for interacting with the Surfline API to fetch forecasts.
    """
    BASE_URL = "https://services.surfline.com/kbyg/spots/forecasts/"

    def __init__(self, params: dict):
        self.params = params
        self.session = requests.Session()

    def fetch(self, forecast_type: str) -> dict:
        """
        Fetches forecast data from the Surfline API.

        Args:
            forecast_type (str): The type of forecast data to fetch.

        Returns:
            dict: The fetched forecast data in dictionary format.
        """
        url = f"{self.BASE_URL}{forecast_type}?{urlencode(self.params)}"
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}\n{response.reason}")
        return json.loads(response.text)