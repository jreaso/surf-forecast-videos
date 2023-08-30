import requests
import os
import ffmpeg
from datetime import datetime


def download_and_process_video(video_url: str, spot_id: str, cam_number) -> str:
    """
    This function downloads a video clip from provided url, cuts the video down to 60s, compresses it and saves it to
    the output directory with appropriate naming. It also changes the creation date of the file.

    :param video_url: link to cdn server where rewind clips are hosted.
    :param spot_id: surf spot id, used in file naming.
    :return: filepath of processed video.
    """
    output_directory = "videos"

    # Directory to save the processed videos
    os.makedirs(output_directory, exist_ok=True)  # Create the output directory if it doesn't exist

    # Download the video
    response = requests.get(video_url, stream=True)

    source_filename = video_url.split("/")[-1]
    video_filename = f"TEMP_{source_filename}"
    video_file_path = os.path.join(output_directory, video_filename)

    with open(video_file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Parse Video Timestamp
    video_timestamp_str = video_url.split(".")[-2]

    # Process and save as new file
    output_filename = f"{spot_id}_{cam_number}_{video_timestamp_str}.mp4"
    output_file_path = os.path.join(output_directory, output_filename)
    crf_value = 28  # Compression rate, default is 24, higher means more compression.

    # Process Video - using ffmpeg
    ffmpeg.input(video_file_path).output(output_file_path, crf=crf_value, t=60, y=None, loglevel="error").run()

    # Convert the timestamp string to a timestamp
    video_timestamp_datetime = datetime.strptime(video_timestamp_str, "%Y%m%dT%H%M%S%f").timestamp()

    # Set creation time
    os.utime(output_file_path, (os.path.getatime(output_file_path), video_timestamp_datetime))

    # Delete the original video
    os.remove(video_file_path)

    return output_file_path
