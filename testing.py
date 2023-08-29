from forecast import Forecast, SurflineWrapper
from db_manager import DBManager
from rewind_clip_scraper import fetch_rewind_links
# from surf_cam_video import ...

params = {
    "spotId": '5842041f4e65fad6a7708d0f',
    "days": 1,
    "intervalHours": 1,
}
api_client = SurflineWrapper()
forecast_data = api_client.fetch_forecast(params)
#print(forecast_data)
forecast = Forecast(forecast_data)
#print(forecast)

#print(forecast.to_dataframe().head())

db_client = DBManager('test')

db_client.insert_surf_spot(('5842041f4e65fad6a7708d0f', 'J-Bay'))

db_client.insert_forecast(forecast)
db_client.close_connection()

print(forecast.to_dataframe()['tide_height'])