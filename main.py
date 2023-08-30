# Fetch Forecast Data
#    Replace any rows in DB
# Scrape Clips
#    Fetch all extensions and spot ids
#    Fill in DB Partially, don't replace current rows - fetch_rewind_links('whitesands/60dc2e530cee140bde3d34f3')
# Check DB for clips to download
#    Which clips need to be downloaded. Mark downloaded, pending, No local
#    Download pending clips - download_and_process_video("https://camrewinds.cdn-surfline.com/live/wa-whitesandsbay.stream.20230829T111029267.mp4", "871273", 1)
#    Update DB for pending clips

from db_manager import DBManager
from forecast import Forecast
from forecast import SurflineWrapper
from rewind_clip_scraper import fetch_rewind_links
from surf_cam_video_processor import download_and_process_video


# Initialize DB Manager
db_manager = DBManager('tmsc-testing')

#


# Fetch Forecast Data


api_client = SurflineWrapper()
forecast_data = api_client.fetch_forecast(params)
