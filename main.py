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
            status = 'Null'  # Default status
            sunlight_dict = db_manager.get_sunlight_times(spot_id, footage_timestamp.date())

            if sunlight_dict:
                sunrise, sunset = (datetime.strptime(sunlight_dict[key], "%Y-%m-%d %H:%M:%S")
                                   for key in ('sunrise', 'sunset'))

                # Check if video is in the light,
                is_light = (sunrise <= footage_timestamp <= sunset)

                # Check if video is taken between the hour and ten past the hour
                hour = footage_timestamp.hour
                minute = footage_timestamp.minute
                is_early = (hour <= minute // 10 <= hour + 1)

                if is_light and is_early:
                    status = 'Pending'

            db_manager.update_cam_video_status((spot_id, cam_number, footage_timestamp), status)


def download_videos() -> None:
    # Get Pending videos
    pass


#update_forecasts()

scrape_clips()

print(db_manager.get_pending_video_links())

#d = db_manager.get_sunlight_times("5842041f4e65fad6a7708bc3", datetime.strptime("2023-08-30", "%Y-%m-%d").date())

#print(d['sunrise'])
#datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")

# Close Connection to DB
db_manager.close_connection()


