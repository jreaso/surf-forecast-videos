from .db_manager import DBManager
from .forecast import Forecast, SurflineWrapper
from .rewind_clip_scraper import fetch_rewind_links
from .surf_cam_video_processor import download_and_process_videos
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
        print('update_forecast() ran successfully')
    except Exception as e:
        logging.error(f'update_forecasts() failed: {str(e)}')
        print(f'update_forecasts() failed: {str(e)}')


def scrape_clips(db_manager: DBManager, headless=True, num_days: int = 5) -> None:
    """
    Scrapes video clip URLs, processes them, and updates the database.

    :param db_manager: An instance of the DBManager class for database operations.
    :param headless: whether to run browser headless or not.
    :param num_days: How many pages to scrape from (how many days back). Max is 5.
    """
    try:
        # Scrape Clips
        surf_cams = db_manager.get_surf_cams()

        cams_list = []  # list of tuples with cam info
        rewind_links_dict = {} # dictionary of urls to scrape from to pass to scraper

        # Cycle Through Cameras
        for surf_cam in surf_cams:
            spot_id, cam_number, rewind_link_extension = surf_cam
            cam = (spot_id, cam_number)

            rewind_links_dict[cam] = rewind_link_extension
            cams_list.append(cam)

        # Get URLs from scraper
        rewind_clip_urls = fetch_rewind_links(rewind_links_dict, headless=headless, num_days=num_days)

        # Check each link and append to DB
        for (spot_id, cam_number), rewind_clip_urls in rewind_clip_urls.items():
            # Insert Links into DB
            for url in rewind_clip_urls:
                # Find the timestamp of the video
                footage_timestamp = datetime.strptime(url.split(".")[-2], "%Y%m%dT%H%M%S%f")

                scraped_link_data = (spot_id, cam_number, footage_timestamp, url)
                db_manager.insert_scraped_video_links(scraped_link_data)

                sunlight_dict = db_manager.get_sunlight_times(spot_id, footage_timestamp.date())

                # Calculate if video should be inserted
                # only insert if the sunlight data is available, the spot was light and it was first clip of the hour
                if sunlight_dict:
                    sunrise, sunset = (datetime.strptime(sunlight_dict[key], "%Y-%m-%d %H:%M:%S")
                                       for key in ('sunrise', 'sunset'))

                    # Check if video is in the light,
                    is_light = (sunrise <= footage_timestamp <= sunset)

                    # Check if video is taken between the hour and ten past the hour
                    is_early = (0 <= footage_timestamp.minute < 10)

                    if is_light and is_early:
                        # Insert clip with Pending status
                        db_manager.update_cam_video_status((spot_id, cam_number, footage_timestamp), 'Pending')

        logging.info('scrape_clips() ran')
        print('scrape_clips() ran')

    except Exception as e:
        logging.error(f'scrape_clips() failed: {str(e)}')
        print(f'scrape_clips() failed: {str(e)}')


def download_videos(db_manager: DBManager) -> None:
    """
    Downloads and processes pending surf_cam_videos, updating the database afterward.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    try:
        # Get Pending entries from surf_cam_videos table
        pending_rows = db_manager.get_pending_video_links()

        # Download and process each video
        download_and_process_videos(db_manager, pending_rows)

        logging.info('download_videos() ran successfully')
        print('download_videos() ran successfully')

    except Exception as e:
        logging.error(f'download_videos() failed: {str(e)}')
        print(f'download_videos() failed: {str(e)}')
