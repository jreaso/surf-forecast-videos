# Scripts

The scripts in this `/scripts` directory run the code from `/src` to scrppae clips, doenload videos, call forecast API and to update the database.

- `run_all.py` is the main script which updates forecast data, scrapes Surfline for video clip links and downloads and processes those links. This can be run from terminal with `full_run_script.sh`.

- `run_forecasts.py` is a lighter script which only updates forecasts. This can be run from terminal with `update_forecast_script.sh`.

- `setup.py` populates the database with surf spots data from `data/surf_spots_data.json`.

- `modify_db.py` is a one time use script to remove J-Bay as a surf spot and remove entries in `cam_videos` table as they were corrupted from a bug. 


The project began running on 01/09/2023 but a bug with downloading and labelling videos caused `modify_db.py` to be run on 16/09/2023 to delete corrupted videos. The videos in `/surf_cam_videos` directory (not tracked by git) were manually deleted.