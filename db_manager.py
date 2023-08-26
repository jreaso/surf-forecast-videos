import sqlite3
from forecast import Forecast


class DBManager:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.cursor = self.conn.cursor()

        if not self._check_schema():
            self._create_tables()

    def _check_schema(self) -> bool:
        tables = ("forecasts", "forecast_swells", "surf_spots", "surf_cams", "cam_footage")

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        table_names_query = "SELECT name FROM sqlite_master WHERE type='table'"
        db_tables = (table[0] for table in self.cursor.execute(table_names_query).fetchall())

        return all(table in db_tables for table in tables)

    def _create_tables(self) -> None:
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        create_tables_query = """
            CREATE TABLE IF NOT EXISTS forecasts (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                forecast_timestamp TIMESTAMP NOT NULL,
                PRIMARY KEY (spot_id, forecast_timestamp),
                -- Foreign Key
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id),
                -- Meta
                utc_offset INTEGER,
                -- Surf
                surf_min FLOAT,
                surf_max FLOAT,
                surf_optimal_score INTEGER,
                surf_human_relation TEXT,
                surf_raw_min FLOAT,
                surf_raw_max FLOAT,
                -- Wind
                wind_speed FLOAT,
                wind_direction FLOAT,
                wind_direction_type TEXT,
                wind_gust FLOAT,
                wind_optimal_score INTEGER,
                -- Probability
                forecast_probability FLOAT,
                -- Tide
                tide_type TEXT,
                tide_height FLOAT,
                -- Weather
                weather_temperature FLOAT,
                weather_condition TEXT,
                weather_pressure FLOAT,
                -- Sunlight
                is_light INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS forecast_swells (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                forecast_timestamp TIMESTAMP NOT NULL,
                swell INTEGER NOT NULL,
                PRIMARY KEY (spot_id, forecast_timestamp, swell),
                -- Foreign Keys
                FOREIGN KEY (spot_id, forecast_timestamp) REFERENCES forecasts(spot_id, forecast_timestamp),
                -- Values
                height FLOAT,
                period FLOAT,
                impact FLOAT,
                power FLOAT,
                direction FLOAT,
                direction_min FLOAT,
                optimal_score INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS surf_spots (
                spot_id TEXT PRIMARY KEY,
                spot_name TEXT,
                region TEXT
            );
            
            CREATE TABLE IF NOT EXISTS surf_cams (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                cam_number INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (spot_id, cam_number),
                -- Foreign Keys
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id),
                -- Data
                cam_name TEXT
                -- Add link and other info for downloading clips
            );
            
            CREATE TABLE IF NOT EXISTS cam_footage (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                cam_number INTEGER NOT NULL DEFAULT 1,
                footage_timestamp TIMESTAMP NOT NULL,
                PRIMARY KEY (spot_id, cam_number, footage_timestamp),
                -- Foreign Keys
                FOREIGN KEY (spot_id, cam_number) REFERENCES surf_cams(spot_id, cam_number),
                -- Link
                footage_link TEXT NOT NULL
            );
        """
        self.cursor.execute(create_tables_query)

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
