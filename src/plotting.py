import pandas as pd
import matplotlib.pyplot as plt

from forecast import Forecast, SurflineWrapper

params = {
    "spotId": '5842041f4e65fad6a7708d0f',
    "days": 1,
    "intervalHours": 3,
}
api_client = SurflineWrapper()
forecast_data = api_client.fetch_forecast(params)
forecast = Forecast(forecast_data)
forecast_df = forecast.to_dataframe()


def plot_forecast(df):
    pass


def plot_forecast_instance(data):
    pass
