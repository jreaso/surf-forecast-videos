import sqlite3
from forecast import Forecast


class DBManager:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def _init_database(self):
        # check tables exist
        pass

    def _check_schema(self):
        # checks table exists
        pass

    def _create_tables(self):
        # creates tables
        pass

    def insert_forecast(self, forecast: list):
        # inserts a forecast object
        # may need to use methods for processing and inserting swells separately
        # may need to check and call methods for inserting spots etc
        pass

    def insert_cam_footage(self, cam_footage):
        # inserts cam footage
        pass

    def close_connection(self):
        self.conn.close()
