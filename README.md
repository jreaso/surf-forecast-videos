# Time Machine Surf Cam

The goal of this project is to be able to show short video clips of what a surf spot is likely to look like in the future based on the surfline forecast for that date. This involves creating a dataset and training an ML model to cluster forecasts together.

The dataset will consist of short labelled videos of surf spots taken periodically from surf cams along with Surfline's forecasts for that spot and time.

We limit this project to a small number of surf spots and cameras. Specifically we will focus on:
- **Jeffreys Bay** (J-Bay), SA. A world famous surf spot with one of the best right hand waves in the world. We will be using two cameras for this spot.
    - [Jeffreys Bay](https://www.surfline.com/surf-cams/jeffreys-bay-j-bay-/5f7ca72ba43acae7a74a4878) - The main camera which views the wave from above.
    - [Jeffreys Bay Front](https://www.surfline.com/surf-cams/jeffreys-bay-j-bay-/62daa32b3fd9a5b33b2130ea) - Secondary camera viewing the waves front on.
- **North Fistral**, Cornwall, UK. One of the UK's top surf spots.
    - [North Fistral Overview](https://www.surfline.com/surf-cams/north-fistral-beach/5a21a0929c7bba001b256978)
- **Whitesands**, Pembrokeshire, UK. A local beach break to me.
    - [Whitesands Bay](https://www.surfline.com/surf-cams/whitesands/60dc2e530cee140bde3d34f3) is the only camera and does not show the main peak (right hand next to rocks).

We also store forecast data only for another spot. No videos are stored due to it not having a camera, but this will leave us the flexibility to integrating photos and videos from other sources (such as social media) in the future to be able to use the tools on this spot also.

- **Fresh West**, Pembrokeshire, UK. Where the Welsh Nationals are held and a particularly consistent spot in Pembrokeshire.

## Requirements

- `FFMPEG`
- Linux or Mac OS. Will not work on windows as modifies video creation date with `os.utime()`.

## Files

- `forecast.py` is based on the `pysurfline` package and has a `SurflineWrapper` object which can be used to fetch the forecast for a spot and then the `Forecast` object can store forecast object and flatten it ready for a dataframe or database.
- `db_manager.py` has a `DBManager` object which manages creating, reading and writing to/from the database.


## Dataset Creation

The first step is to build a system to fetch cam footage. We want to download a 1-minute clip (at a reduced framerate for storage) every hour during daylight. We will be pulling clips from Surfline's cam rewind feature. Once daily, we download all relevant clips in the last 24h. We will then need to process them and put them into storage. For storage reasons, clips will need to be cut down and their framerate will need to be reduced.

Alongside this we will query Surfline's forecast API and store the results in a database. Since the clips of the surf are hourly like the forecast data, we also want to include links to the video clips along with the forecast data. 

