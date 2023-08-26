import sqlite3
from forecast import Forecast


class DBManager:
    """
    Manages creating, reading and writing to/from the db.
    """
    def __init__(self, db_name: str) -> None:
        """
        Initialize the DBManager with the given database name, sets parameter for foreign keys to be used and checks
        database has the correct schema.

        :param db_name: The name of the SQLite database.
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.cursor = self.conn.cursor()

        self.log = []

        if not self._check_schema():
            self.create_tables()

    def _check_schema(self) -> bool:
        """
        Check if the required tables exist in the database schema.

        :return: True if all required tables exist, False otherwise.
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
        create_table_queries = ("""
            CREATE TABLE IF NOT EXISTS forecasts (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                forecast_timestamp TIMESTAMP NOT NULL,
                -- Meta
                utc_offset INTEGER,
                -- Surf
                surf_min REAL,
                surf_max REAL,
                surf_optimal_score INTEGER,
                surf_human_relation TEXT,
                surf_raw_min REAL,
                surf_raw_max REAL,
                -- Wind
                wind_speed REAL,
                wind_direction REAL,
                wind_direction_type TEXT,
                wind_gust REAL,
                wind_optimal_score INTEGER,
                -- Probability
                forecast_probability REAL,
                -- Tide
                tide_type TEXT,
                tide_height REAL,
                -- Weather
                weather_temperature REAL,
                weather_condition TEXT,
                weather_pressure REAL,
                -- Sunlight
                is_light INTEGER,
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, forecast_timestamp),
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id)
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS forecast_swells (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                forecast_timestamp TIMESTAMP NOT NULL,
                swell INTEGER NOT NULL,
                -- Values
                height REAL,
                period REAL,
                impact REAL,
                power REAL,
                direction REAL,
                direction_min REAL,
                optimal_score INTEGER,
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, forecast_timestamp, swell),
                FOREIGN KEY (spot_id, forecast_timestamp) REFERENCES forecasts(spot_id, forecast_timestamp)
            )
        """,
        """ 
            CREATE TABLE IF NOT EXISTS surf_spots (
                spot_id TEXT PRIMARY KEY,
                spot_name TEXT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS surf_cams (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                cam_number INTEGER NOT NULL DEFAULT 1,
                -- Data
                cam_name TEXT,
                -- Add link and other info for downloading clips
                -- ...
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, cam_number),
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id)
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS cam_footage (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                cam_number INTEGER NOT NULL DEFAULT 1,
                footage_timestamp TIMESTAMP NOT NULL,
                -- Link
                footage_link TEXT NOT NULL,
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, cam_number, footage_timestamp),
                FOREIGN KEY (spot_id, cam_number) REFERENCES surf_cams(spot_id, cam_number)
            )
        """)

        for query in create_table_queries:
            self.cursor.execute(query)

        self.conn.commit()

        self.log.append('created tables')

    def insert_forecast(self, forecast: Forecast) -> None:
        """
        Insert forecast data into the database from a forecast object.

        :param forecast: forecast object
        """
        for forecast_entry in forecast.flatten():
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
            self.log.append(f'committed forecast insert ({timestamp}) transaction')

    def _insert_surf_spot_if_not_exists(self, spot_id: str) -> None:
        """
        Insert a surf spot into the surf_spots table if it doesn't already exist. This will not add the spot name.

        :param spot_id: The ID of the surf spot.
        """
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        select_query = "SELECT spot_id FROM surf_spots WHERE spot_id = ?"
        existing_spot = self.cursor.execute(select_query, (spot_id,)).fetchone()

        if not existing_spot:
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            insert_query = "INSERT INTO surf_spots (spot_id) VALUES (?)"
            self.cursor.execute(insert_query, (spot_id,))

            self.log.append(f'added missing spot {spot_id} to surf_spots')

    def _insert_into_forecasts_table(self, forecast_entry: dict) -> None:
        """
        Insert forecast data (except swells data) into the forecasts table.

        :param forecast_entry: A dictionary containing a single instance of forecast data.
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

        values = [forecast_entry.get(key, None) for key in keys]

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = (
            f"INSERT OR REPLACE INTO forecasts ({', '.join(columns)})"
            f" VALUES ({', '.join(['?'] * len(keys))})"
        )
        self.cursor.execute(insert_query, values)

        self.log.append('inserted data into forecasts table')

    def _insert_into_forecast_swells_table(self, forecast_entry: dict) -> None:
        """
        Insert forecast swell data into the forecast_swells table.

        :param forecast_entry: A dictionary containing a single instance of forecast data.
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
        insert_query = (
            f"INSERT OR REPLACE INTO forecast_swells ({', '.join(columns)})"
            f" VALUES ({', '.join(['?'] * len(columns))})"
        )
        self.cursor.executemany(insert_query, values)

        self.log.append('inserted data into forecast_swells table')

    def insert_surf_spot(self, surf_spot: tuple) -> None:
        """
        Insert a surf spot into the surf_spots table.

        :param surf_spot: A tuple containing surf spot information (spot_id, spot_name).
        """
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = "INSERT OR REPLACE INTO surf_spots (spot_id, spot_name) VALUES (?, ?)"
        self.cursor.execute(insert_query, surf_spot)
        self.conn.commit()

        self.log.append('inserted surf spot to surf_spots table and committed transaction')

    def insert_cam_footage(self, cam_footage):
        """
        INCOMPLETE
        Insert cam footage data information into the cam_footage table.
        """
        pass

    def close_connection(self) -> None:
        """
        Close the database connection.
        """
        self.conn.close()

        self.log.append('closed connection')
