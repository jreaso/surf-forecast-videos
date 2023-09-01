from .db_manager import DBManager
from .forecast import Forecast, SurflineWrapper
from .rewind_clip_scraper import fetch_rewind_links
from .surf_cam_video_processor import download_and_process_video
from datetime import datetime
import logging


def update_forecasts(db_manager: DBManager) -> None:
    """
    Fetches forecast data and updates the database with the new forecast information.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    try:
        # Cycle through surf spots
        surf_spot_ids = db_manager.get_surf_spot_ids()

        for spot_id in surf_spot_ids:
            # Get Forecast Data and Insert into DB
            params = {
                "spotId": spot_id,
                "days": 5,
                "intervalHours": 1,
            }
            api_client = SurflineWrapper()  # For Fetching Forecast Data
            forecast_data = api_client.fetch_forecast(params)
            forecast = Forecast(forecast_data)
            db_manager.insert_forecast(forecast)

        logging.info('update_forecast() ran successfully')
    except Exception as e:
        logging.error(f'update_forecasts() failed: {str(e)}')


def scrape_clips(db_manager: DBManager) -> None:
    """
    Scrapes video clip URLs, processes them, and updates the database.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    try:
        # Scrape Clips
        surf_cams = db_manager.get_surf_cams()

        cams_list = []  # list of tuples with cam info
        rewind_link_extensions_list = []  # list of urls to scrape from to pass to scraper

        # Cycle Through Cameras
        for surf_cam in surf_cams:
            spot_id, cam_number, rewind_link_extension = surf_cam

            rewind_link_extensions_list.append(rewind_link_extension)
            cams_list.append((spot_id, cam_number))

        # Get URLs from scraper
        rewind_clip_urls_all = fetch_rewind_links(rewind_link_extensions_list, headless=True)

        # Check each link and append to DB
        for (spot_id, cam_number), rewind_clip_urls in zip(cams_list, rewind_clip_urls_all):
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
                    is_early = (0 <= footage_timestamp.minute < 10)

                    if is_light and is_early:
                        status = 'Pending'

                db_manager.update_cam_video_status((spot_id, cam_number, footage_timestamp), status)

        logging.info('scrape_clips() ran successfully')

    except Exception as e:
        logging.error(f'scrape_clips() failed: {str(e)}')


def download_videos(db_manager: DBManager) -> None:
    """
    Downloads and processes pending surf_cam_videos, updating the database afterward.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    try:
        # Get Pending surf_cam_videos
        pending_rows = db_manager.get_pending_video_links()
        for row in pending_rows:
            spot_id, cam_number, footage_timestamp, video_url = row
            # Download and Process Video
            video_file_path = download_and_process_video(video_url, spot_id, cam_number)
            # Update DB
            db_manager.insert_downloaded_videos((spot_id, cam_number, footage_timestamp), video_file_path)

        logging.info('download_videos() ran successfully')

    except Exception as e:
        logging.error(f'download_videos() failed: {str(e)}')
