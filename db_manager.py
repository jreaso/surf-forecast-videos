import sqlite3
from forecast import Forecast
import datetime


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
        tables = ("forecasts", "forecast_swells", "surf_spots", "surf_cams", "cam_videos")

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
            CREATE TABLE IF NOT EXISTS sunlight_times (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                date DATE NOT NULL,
                -- Values
                midnight TIMESTAMP,
                dawn TIMESTAMP,
                sunrise TIMESTAMP,
                sunset TIMESTAMP,
                dusk TIMESTAMP,
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, date),
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id)
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
                rewind_link_extension TEXT,
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, cam_number),
                FOREIGN KEY (spot_id) REFERENCES surf_spots(spot_id)
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS cam_videos (
                -- Primary Keys
                spot_id TEXT NOT NULL,
                cam_number INTEGER NOT NULL DEFAULT 1,
                footage_timestamp TIMESTAMP NOT NULL,
                -- Forecast
                forecast_timestamp TEXT,
                -- Link to CDN Server
                download_link TEXT,
                -- Video Location on Local Machine
                video_storage_location TEXT,
                -- Status for whether link is pending download,downloaded or not being downloaded
                download_status TEXT, -- Pending, Downloaded or Null
                -- Primary and Foreign Keys
                PRIMARY KEY (spot_id, cam_number, footage_timestamp),
                FOREIGN KEY (spot_id, cam_number) REFERENCES surf_cams(spot_id, cam_number),
                FOREIGN KEY (spot_id, forecast_timestamp) REFERENCES forecasts(spot_id, forecast_timestamp)
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
        flat_forecast = forecast.flatten()
        spot_id = flat_forecast[0]['spot_id']

        for forecast_entry in flat_forecast:
            # Check if the surf spot exists in the database, if not, insert it
            self._insert_surf_spot_if_not_exists(spot_id)

            # Insert all forecast data except swell data into forecasts table
            self._insert_into_forecasts_table(forecast_entry)

            # Insert forecast swells into forecast_swells table
            self._insert_into_forecast_swells_table(forecast_entry)

        # Insert sunlight times into sunlight_times tables
        self._insert_into_sunlight_times_table(spot_id, forecast.sunlight_times(add_date=True))

        # Commit the transactions
        self.conn.commit()
        self.log.append(f'committed forecast insert transactions')

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
            insert_query = "INSERT OR REPLACE INTO surf_spots (spot_id) VALUES (?)"
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
            'weather_temperature', 'weather_condition', 'weather_pressure'
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

    def _insert_into_sunlight_times_table(self, spot_id: str, sunlight_times: list) -> None:
        """
        Insert sunlight times data into the sunlight_times table.

        :param spot_id: surf spot id string.
        :param sunlight_times: A list of dictionaries containing sunlight times data.
        """
        for sunlight_dict in sunlight_times:
            insert_query = """
                INSERT OR REPLACE INTO sunlight_times (spot_id, date, midnight, dawn, sunrise, sunset, dusk) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            order = ('date', 'midnight', 'dawn', 'sunrise', 'sunset', 'dusk')
            data = (spot_id, ) + tuple(sunlight_dict[key] for key in order)

            self.cursor.execute(insert_query, data)

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

    def insert_surf_cam(self, surf_cam: tuple) -> None:
        """
        Insert a surf camera into the surf_spots table.

        :param surf_cam: A tuple containing surf cam info (spot_id, cam_number, cam_name, rewind_link_extension).
        """
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        insert_query = (
            "INSERT OR REPLACE INTO surf_cams (spot_id, cam_number, cam_name, rewind_link_extension) "
            "VALUES (?, ?, ?, ?)"
        )
        self.cursor.execute(insert_query, surf_cam)
        self.conn.commit()

        self.log.append(f"inserted surf cam to surf_cams table and committed transaction")

    def get_sunlight_times(self, spot_id: str, date: datetime.date):
        """
        Get sunlight times for a date and spot.

        :return: dictionary of sunlight times.
        """
        select_query = f"""
            SELECT midnight, dawn, sunrise, sunset, dusk FROM sunlight_times
            WHERE spot_id = '{spot_id}' AND date = '{date}'
        """
        self.cursor.execute(select_query)

        rows = self.cursor.fetchall()
        if rows:
            row = rows[0]
            sunlight_times_dict = {key: row[i]
                                   for i, key in enumerate(['midnight', 'dawn', 'sunrise', 'sunset', 'dusk'])}
            return sunlight_times_dict
        else:
            return None

    def get_surf_cams(self) -> list:
        """
        Get all surf cameras in the db.

        :return: list of rows in surf_cams table.
        """
        select_query = "SELECT spot_id, cam_number, rewind_link_extension FROM surf_cams"
        self.cursor.execute(select_query)

        surf_cam_rows = self.cursor.fetchall()
        return surf_cam_rows

    def get_surf_spot_ids(self) -> list:
        """
        Get all surf spots in the db.

        :return: list of rows in surf_spots table.
        """
        select_query = "SELECT spot_id FROM surf_spots"
        self.cursor.execute(select_query)

        surf_spot_rows = self.cursor.fetchall()
        surf_spot_ids = [row[0] for row in surf_spot_rows]
        return surf_spot_ids

    def insert_scraped_video_links(self, scraped_link_data: tuple) -> None:
        """
        Inserts scraped video link data into the cam_videos table.

        :param scraped_link_data: A tuple containing the scraped video link information in the following order:
            - spot_id_value (str): The identifier of the surf spot.
            - cam_number_value (int): The number of the camera.
            - footage_timestamp_value (datetime): The timestamp of the footage.
            - download_link_value (str): The URL of the video download link.
        """
        insert_query = """
            INSERT OR REPLACE INTO cam_videos (spot_id, cam_number, footage_timestamp, download_link, download_status)
            VALUES (?, ?, ?, ?, 'Pending')
        """

        self.cursor.execute(insert_query, scraped_link_data)
        self.conn.commit()
        self.log.append(f"inserted scraped cam footage link into cam_videos table and committed transaction")

    def update_cam_video_status(self, cam_video_entry: tuple, download_status: str) -> None:
        """
        Update the download_status of a row in the cam_videos table.

        :param cam_video_entry: A tuple containing the primary key information for the row.
            - spot_id
            - cam_number
            - footage_timestamp
        :param download_status: A string representing the new download status value.
        """
        update_query = """
            UPDATE cam_videos
            SET download_status = ?
            WHERE spot_id = ? AND cam_number = ? AND footage_timestamp = ?
        """

        data = (download_status,) + cam_video_entry

        self.cursor.execute(update_query, data)
        self.conn.commit()
        self.log.append(f"updated download_status column in row in cam_videos table and committed transaction")

    def get_pending_video_links(self) -> list:
        """
        Get the primary key information of rows with download_status 'Pending'.

        :return: A list of tuples, each containing the primary key information (spot_id, cam_number, footage_timestamp).
        """
        select_query = """
            SELECT spot_id, cam_number, footage_timestamp
            FROM cam_videos
            WHERE download_status = 'Pending'
        """

        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()

        return rows

    def insert_downloaded_videos(self, cam_video_entry: tuple, video_storage_location: str) -> None:
        """
        INCOMPLETE
        """
        update_query = """
            UPDATE cam_videos
            SET download_status = 'Downloaded', video_storage_location = ?
            WHERE spot_id = ? AND cam_number = ? AND footage_timestamp = ?
        """

        data = (video_storage_location,) + cam_video_entry

        self.cursor.execute(update_query, data)
        self.conn.commit()
        self.log.append(f"updated download_status column in row in cam_videos table and committed transaction")

    def close_connection(self) -> None:
        """
        Close the database connection.
        """
        self.conn.close()

        self.log.append('closed connection')
