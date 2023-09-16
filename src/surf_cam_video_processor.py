import requests
import os
import ffmpeg
import subprocess
from datetime import datetime
from .db_manager import DBManager


def process_video(url, output_file_path):
    crf_value = 28  # Compression rate, default is 24, higher means more compression.
    
    # Use FFmpeg to directly process the video and limit it to 60 seconds
    cmd = [
        'ffmpeg',
        '-i', url,               # Input URL
        '-t', '60',              # Limit duration to 60 seconds
        '-crf', str(crf_value),  # Compression rate
        '-y',                    # Overwrite output file if it exists
        '-loglevel', 'error',    # Suppress FFmpeg log messages
        output_file_path
    ]

    subprocess.run(cmd, check=True)

def download_and_process_videos(db_manager: DBManager, pending_rows: list) -> None:
    """
    This function downloads video clips from provided urls, cuts the video down to 60s, compresses it and saves it to
    the output directory with appropriate naming. It also changes the creation date of the files.

    :param db_manager: An instance of the DBManager class for database operations.
    :param pending_rows: A list of tuples that are rows from pending entries in `cam_videos` table. Each tuple in the list contains:
        - spot_id: surf spot id, used in file naming.
        - cam_number: number of the cam at the spot, used in file naming.
        - footage_timestamp: timestamp of video clip.
        - video_url: link to cdn server where rewind clips are hosted.
    """
    output_directory = os.path.join("..", "surf_cam_videos")

    # Directory to save the processed surf_cam_videos
    os.makedirs(output_directory, exist_ok=True)  # Create the output directory if it doesn't exist

    for spot_id, cam_number, footage_timestamp, video_url  in pending_rows:
        """
        # Download the video
        response = requests.get(video_url, stream=True)
        response.raise_for_status() # Raise an exception if the request fails

        source_filename = video_url.split("/")[-1]
        video_file_path = os.path.join(output_directory, f"TEMP_{source_filename}")

        with open(video_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        """



        # Process video
        video_timestamp_str = video_url.split(".")[-2]

        output_filename = f"{spot_id}_{cam_number}_{video_timestamp_str}.mp4"
        output_file_path = os.path.join(output_directory, output_filename)

        """
        crf_value = 28  # Compression rate, default is 24, higher means more compression.

        # Process Video - using ffmpeg
        ffmpeg.input(video_file_path).output(output_file_path, crf=crf_value, t=60, y=None, loglevel="error").run()
        """

        process_video(video_url, output_file_path)

        # Convert the timestamp string to a timestamp
        video_timestamp_datetime = datetime.strptime(video_timestamp_str, "%Y%m%dT%H%M%S%f").timestamp()

        # Set creation time
        os.utime(output_file_path, (os.path.getatime(output_file_path), video_timestamp_datetime))

        # Delete the original video
        #os.remove(video_file_path)

        # Update DB
        db_manager.insert_downloaded_videos((spot_id, cam_number, footage_timestamp), output_file_path)
