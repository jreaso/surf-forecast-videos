from src.core import download_videos
from src.db_manager import DBManager

# Initialize a DB Manager
db_manager = DBManager('SurfForecastDB')

# Download videos
download_videos(db_manager)

# Close Connection to DB
db_manager.close_connection()