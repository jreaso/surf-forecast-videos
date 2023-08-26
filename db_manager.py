import sqlite3
from forecast import Forecast


class DBManager:
    def __init__(self, db_name: str) -> None:
        """
        Initialize the DBManager with the given database name, sets parameter for foreign keys to be used and checks
        database has the correct schema.

        Args:
            db_name (str): The name of the SQLite database.
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.cursor = self.conn.cursor()

        if not self._check_schema():
            self.create_tables()

    def _check_schema(self) -> bool:
        """
        Check if the required tables exist in the database schema.

        Returns:
            bool: True if all required tables exist, False otherwise.
        """
        tables = ("forecasts", "forecast_swells", "surf_spots", "surf_cams", "cam_footage")

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        table_names_query = "SELECT name FROM sqlite_master WHERE type='table'"
        db_tables = (table[0] for table in self.cursor.execute(table_names_query).fetchall())

        return all(table in db_tables for table in tables)

    def create_tables(self) -> None:
        """
        Create the required tables for the database if they do not (all) exist.
        """
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
                spot_name TEXT
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
        """
        Insert forecast data into the database from a forecast object.

        Args:
            forecast (list): List of forecast data entries as dictionaries.
        """
        for forecast_entry in forecast:
            spot_id = forecast_entry['spot_id']
            timestamp = forecast_entry['timestamp']

            # Check if the surf spot exists in the database, if not, insert it
            self._insert_surf_spot_if_not_exists(spot_id)

            # Insert all forecast data except swell data into forecasts table
            self._insert_into_forecasts_table(forecast_entry)

            # Insert forecast swells into forecast_swells table
            self._insert_into_forecast_swells_table(forecast_entry)

            # Commit the transaction
            self.conn.commit()

    def _insert_surf_spot_if_not_exists(self, spot_id: str) -> None:
        """
        Insert a surf spot into the surf_spots table if it doesn't already exist. This will not add the spot name.

        Args:
            spot_id (str): The ID of the surf spot.
        """
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        select_query = "SELECT spot_id FROM surf_spots WHERE spot_id = ?"
        existing_spot = self.cursor.execute(select_query, (spot_id,)).fetchone()

        if not existing_spot:
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            insert_query = "INSERT INTO surf_spots (spot_id) VALUES (?)"
            self.cursor.execute(insert_query, (spot_id,))

    def _insert_into_forecasts_table(self, forecast_entry: dict) -> None:
        """
        Insert forecast data (except swells data) into the forecasts table.

        Args:
            forecast_entry (dict): A dictionary containing a single instance of forecast data.
        """
        columns = [
            'spot_id', 'forecast_timestamp', 'utc_offset', 'surf_min', 'surf_max',
            'surf_optimal_score', 'surf_human_relation', 'surf_raw_min', 'surf_raw_max',
            'wind_speed', 'wind_direction', 'wind_direction_type', 'wind_gust',
            'wind_optimal_score', 'forecast_probability', 'tide_type', 'tide_height',
            'weather_temperature', 'weather_condition', 'weather_pressure', 'is_light'
        ]
        # `forecast_timestamp` column (in forecasts table) is equivalent to `timestamp` key (in forecast dict)
        keys = [col if col != 'forecast_timestamp' else 'timestamp' for col in columns]

        values = [forecast_entry[key] for key in keys]

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = f"INSERT INTO forecasts ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(keys))})"
        self.cursor.execute(insert_query, values)

    def _insert_into_forecast_swells_table(self, forecast_entry: dict) -> None:
        """
        Insert forecast swell data into the forecast_swells table.

        Args:
            forecast_entry (dict): A dictionary containing a single instance of forecast data.
        """
        n = len(forecast_entry['swell_height'])

        columns = ('spot_id', 'forecast_timestamp', 'swell', 'height', 'period', 'impact', 'power', 'direction',
                   'direction_min', 'optimal_score')

        values = zip(
            [forecast_entry['spot_id']] * n,
            [forecast_entry['timestamp']] * n,
            range(1, 7),
            forecast_entry['swell_height'],
            forecast_entry['swell_period'],
            forecast_entry['swell_impact'],
            forecast_entry['swell_power'],
            forecast_entry['swell_direction'],
            forecast_entry['swell_direction_min'],
            forecast_entry['swell_optimal_score']
        )

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = f"INSERT INTO forecast_swells ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        self.cursor.executemany(insert_query, values)

    def insert_surf_spot(self, surf_spot: tuple) -> None:
        """
        Insert a surf spot into the surf_spots table.

        Args:
            surf_spot (tuple): A tuple containing surf spot information (spot_id, spot_name).
        """
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = "INSERT OR REPLACE INTO surf_spots (spot_id, spot_name) VALUES (?, ?)"
        self.cursor.execute(insert_query, surf_spot)
        self.conn.commit()

    def insert_cam_footage(self, cam_footage):
        """
        INCOMPLETE
        Insert cam footage data information into the cam_footage table.
        """
        pass

    def close_connection(self):
        """
        Close the database connection.
        """
        self.conn.close()
