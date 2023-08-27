# 1. Fetch a clip for a spot, and datetime
# 2. Process clip: edit framerate and cut to 60s
# 3. Edit clip metadata (creation date etc)
# 4. Save to file

import requests
import os
import ffmpeg
from datetime import datetime



# DOWNLOAD VIDEO (Placeholder)

# URL of the video to download
video_url = "https://camrewinds.cdn-surfline.com/live/za-jeffreysbay.stream.20230827T151248661.mp4"

# Directory to save the processed videos
output_directory = "videos"
# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Download the video
response = requests.get(video_url, stream=True)
video_filename = os.path.join(output_directory, "original_video.mp4")

with open(video_filename, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)


# PROCESS VIDEO - using ffmpeg

processed_filename = os.path.join(output_directory, "processed_video.mp4")

#compression, 24 is default
crf_value = 28  # You can adjust this value higher for more compression

# Process the video with modified metadata
ffmpeg.input(video_filename).output(processed_filename, crf=crf_value, t=60, y=None, loglevel="error").run()

file_path = "/Users/jamiereason/programming-projects/time-machine-surf-cam/videos/processed_video.mp4"
new_creation_time = "2008-08-01 12:00:00"

# Convert the new_creation_time to a datetime object
new_creation_time = datetime.strptime(new_creation_time, "%Y-%m-%d %H:%M:%S")
new_access_time = new_modification_time = new_creation_time.timestamp()

# Set access time, modification time, and creation time (not directly modifiable)
os.utime(file_path, (new_access_time, new_modification_time))

print("Video processing complete. Processed video with modified creation date saved at:", processed_filename)



