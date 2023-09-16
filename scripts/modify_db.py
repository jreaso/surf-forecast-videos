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
    Removes all entries from `sunlight_times`, `surf_cams`, `surf_spots`, `forecasts` and `forecast_swells` tables where the spot_id is "5842041f4e65fad6a7708d0f" (J-Bay).

    The rest of the spot forecasts are left.

    :param db_manager: An instance of the DBManager class for database operations.
    """
    queries = (
        "DELETE FROM sunlight_times WHERE spot_id = '5842041f4e65fad6a7708d0f'",
        "DELETE FROM surf_cams WHERE spot_id = '5842041f4e65fad6a7708d0f'",
        "DELETE FROM forecast_swells WHERE spot_id = '5842041f4e65fad6a7708d0f'",
        "DELETE FROM forecasts WHERE spot_id = '5842041f4e65fad6a7708d0f'",
    )

    for query in queries:
        db_manager.run_query(query, commit=False)

    query = "DELETE FROM surf_spots WHERE spot_id = '5842041f4e65fad6a7708d0f'"

    db_manager.run_query(query, commit=True)


if __name__ == "__main__":
    surf_db_manager = DBManager('SurfForecastDB')

    remove_all_cam_videos(surf_db_manager)
    #remove_cam_forecasts(surf_db_manager)

    surf_db_manager.close_connection()
