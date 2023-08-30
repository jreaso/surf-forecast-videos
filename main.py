from db_manager import DBManager
from forecast import Forecast
from forecast import SurflineWrapper
from rewind_clip_scraper import fetch_rewind_links
from surf_cam_video_processor import download_and_process_video
from datetime import datetime


# Initialize DB Manager
db_manager = DBManager('tmsc-testing')
# First time running - run `setup.py`

api_client = SurflineWrapper()  # For Fetching Forecast Data


def update_forecasts() -> None:
    # Cycle through surf spots
    surf_spot_ids = db_manager.get_surf_spot_ids()

    for spot_id in surf_spot_ids:
        # Get Forecast Data and Insert into DB
        params = {
            "spotId": spot_id,
            "days": 5,
            "intervalHours": 1,
        }
        forecast_data = api_client.fetch_forecast(params)
        forecast = Forecast(forecast_data)
        db_manager.insert_forecast(forecast)


def scrape_clips() -> None:
    # Scrape Clips
    surf_cams = db_manager.get_surf_cams()

    # Cycle Through Cameras
    for surf_cam in surf_cams:
        spot_id, cam_number, rewind_link_extension = surf_cam
        rewind_clip_urls = fetch_rewind_links(rewind_link_extension)

        # Insert Links into DB
        for url in rewind_clip_urls:
            # Find the timestamp of the video
            footage_timestamp = datetime.strptime(url.split(".")[-2], "%Y%m%dT%H%M%S%f")

            scraped_link_data = (spot_id, cam_number, footage_timestamp, url)
            db_manager.insert_scraped_video_links(scraped_link_data)

            # Calculate Label for Video
            ...

            # Update Label
            ...

def download_videos() -> None:
    pass




#    Fetch all extensions and spot ids
#    Fill in DB Partially, don't replace current rows - fetch_rewind_links('whitesands/60dc2e530cee140bde3d34f3')


# Download Videos


# Close Connection to DB
db_manager.close_connection()

