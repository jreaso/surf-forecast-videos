# Duplicate of `run_all.py` with `headless=False`

from src.core import update_forecasts, scrape_clips, download_videos
from src.db_manager import DBManager

# Initialize a DB Manager
db_manager = DBManager('SurfForecastDB')

# Run all functions
update_forecasts(db_manager)
scrape_clips(db_manager, False, 5)
download_videos(db_manager)

# Close Connection to DB
db_manager.close_connection()