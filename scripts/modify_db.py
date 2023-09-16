from src.db_manager import DBManager

def remove_all_cam_videos(db_manager: DBManager) -> None:
    """
    Removes all entries in cam_videos table.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    query = "DELETE FROM cam_videos"

    db_manager.run_query(query)

def remove_cam_forecasts(db_manager: DBManager) -> None:
    """
    Removes all entries from `sunlight_times`, `surf_cams`, `surf_spots`, `forecasts` and `forecast_swells` tables where the spot_id is "5842041f4e65fad6a7708d0f" (J-Bay) or "5842041f4e65fad6a7708bc3" (Supertubos).

    :param db_manager: An instance of the DBManager class for database operations.
    """
    queries = (
        "DELETE FROM sunlight_times WHERE spot_id IN ('5842041f4e65fad6a7708d0f', '5842041f4e65fad6a7708bc3')",
        "DELETE FROM surf_cams WHERE spot_id IN ('5842041f4e65fad6a7708d0f', '5842041f4e65fad6a7708bc3')",
        "DELETE FROM forecasts WHERE spot_id IN ('5842041f4e65fad6a7708d0f', '5842041f4e65fad6a7708bc3')",
        "DELETE FROM forecast_swells WHERE spot_id IN ('5842041f4e65fad6a7708d0f', '5842041f4e65fad6a7708bc3')",
    )

    for query in queries:
        db_manager.run_query(query, commit=False)

    query = """
        DELETE FROM surf_spots WHERE spot_id IN ('5842041f4e65fad6a7708d0f', '5842041f4e65fad6a7708bc3')
    """

    db_manager.run_query(query, commit=True)
