from src.core import update_forecasts, scrape_clips, download_videos
from src.db_manager import DBManager

# Initialize a DB Manager
db_manager = DBManager('SurfForecastDB')

# Run all functions
update_forecasts(db_manager)
scrape_clips(db_manager)
download_videos(db_manager)

# Close Connection to DB
db_manager.close_connection()