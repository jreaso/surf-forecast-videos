from forecast import Forecast, SurflineWrapper
from db_manager import DBManager

params = {
    "spotId": '584204204e65fad6a77090cf',
    "days": 1,
    "intervalHours": 1,
}
api_client = SurflineWrapper()
forecast_data = api_client.fetch_forecast(params)
forecast = Forecast(forecast_data)

db_client = DBManager('test')

db_client.insert_forecast(forecast)
db_client.close_connection()