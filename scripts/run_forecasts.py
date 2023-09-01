from src.core import update_forecasts
from src.db_manager import DBManager

# Initialize a DB Manager
db_manager = DBManager('SurfForecastDB')

# Run only the update_forecasts function
update_forecasts(db_manager)

# Close Connection to DB
db_manager.close_connection()
