from forecast import Forecast, SurflineWrapper
from db_manager import DBManager

params = {
    "spotId": '5842041f4e65fad6a7708d0f',
    "days": 1,
    "intervalHours": 1,
}
api_client = SurflineWrapper()
forecast_data = api_client.fetch_forecast(params)
forecast = Forecast(forecast_data)

db_client = DBManager('test')

db_client.insert_surf_spot(('5842041f4e65fad6a7708d0f', 'J-Bay'))

db_client.insert_forecast(forecast)
db_client.close_connection()

print(forecast.to_dataframe()['tide_height'])