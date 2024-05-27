import sqlalchemy
import pandas as pd
from sqlalchemy.sql import text


class DBManager:
    def __init__(self, data):
        self.engine = None
        self.place: sqlalchemy.Table = None
        self.observation: sqlalchemy.Table = None
        self.temperature: sqlalchemy.Table = None
        self.data: pd.DataFrame = data

    # Connect to the database using the connection string. Replace the password with your own password.
    def init_db_connection(self):
        self.engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://postgres:password@localhost:5432"
        )
        connection = self.engine.connect()
        connection.execute(sqlalchemy.text("COMMIT;"))
        connection.execute(sqlalchemy.text("DROP DATABASE IF EXISTS weatherdata;"))
        connection.execute(sqlalchemy.text("CREATE DATABASE weatherdata WITH ENCODING 'UTF8';"))
        connection.close()

        self.engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://postgres:password@localhost:5432/weatherdata"
        )

        meta = sqlalchemy.MetaData()

        # Create tables Place, Observation, and Temperature according to the following definitionsCreate tables Place, Observation, and Temperature according to the following definitions
        # Place (code, name, latitude, longitude)
        # Observation (place, date, rain, snow, air temperature, ground temperature)
        # Temperature (place, date, lowest, highest)

        self.place = sqlalchemy.Table(
            "place",
            meta,
            sqlalchemy.Column("code", sqlalchemy.String, primary_key=True),
            sqlalchemy.Column("name", sqlalchemy.String),
            sqlalchemy.Column("latitude", sqlalchemy.Float),
            sqlalchemy.Column("longitude", sqlalchemy.Float),
        )

        self.place.create(self.engine, checkfirst=True)

        self.observation = sqlalchemy.Table(
            "observation",
            meta,
            sqlalchemy.Column(
                "place", sqlalchemy.String, sqlalchemy.ForeignKey("place.code")
            ),
            sqlalchemy.Column("date", sqlalchemy.Date),
            sqlalchemy.Column("rain", sqlalchemy.Float),
            sqlalchemy.Column("snow", sqlalchemy.Float),
            sqlalchemy.Column("air_temperature", sqlalchemy.Float),
            sqlalchemy.Column("ground_temperature", sqlalchemy.Float),
            sqlalchemy.PrimaryKeyConstraint("place", "date"),
        )

        self.observation.create(self.engine, checkfirst=True)

        self.temperature = sqlalchemy.Table(
            "temperature",
            meta,
            sqlalchemy.Column(
                "place", sqlalchemy.String, sqlalchemy.ForeignKey("place.code")
            ),
            sqlalchemy.Column("date", sqlalchemy.Date),
            sqlalchemy.Column("lowest", sqlalchemy.Float),
            sqlalchemy.Column("highest", sqlalchemy.Float),
            sqlalchemy.PrimaryKeyConstraint("place", "date"),
        )

        self.temperature.create(self.engine, checkfirst=True)

    # Insert the data for Place, Observation, and Temperature tables
    def insert_place(self):
        # Retrieve the unique places from the data
        places = self.data[
            ["place", "place_code", "latitude", "longitude"]
        ].drop_duplicates()
        # Insert the places into the Place table
        with self.engine.connect() as connection:
            for index, row in places.iterrows():
                session = connection.begin()
                stmt = sqlalchemy.insert(self.place).values(
                    code=row["place_code"],
                    name=row["place"],
                    latitude=row["latitude"],
                    longitude=row["longitude"],
                )
                connection.execute(stmt)
                session.commit()

    def insert_observation(self):
        # Insert the observations into the Observation table
        with self.engine.connect() as connection:
            for index, row in self.data.iterrows():
                session = connection.begin()
                stmt = sqlalchemy.insert(self.observation).values(
                    place=row["place_code"],
                    date=f"{row['year']}-{row['month']}-{row['day']}",
                    rain=(
                        None if row["rain"] == "NULL" else row["rain"]
                    ),  # Replace "NULL" with "None
                    snow=(
                        None if row["snow"] == "NULL" else row["snow"]
                    ),  # Replace "NULL" with "None
                    air_temperature=(
                        None
                        if row["air_temperature"] == "NULL"
                        else row["air_temperature"]
                    ),  # Replace "NULL" with "None
                    ground_temperature=(
                        None
                        if row["ground_temperature"] == "NULL"
                        else row["ground_temperature"]
                    ),  # Replace "NULL" with "None
                )
                connection.execute(stmt)
                session.commit()

    def insert_temperature(self):
        # Insert the temperatures into the Temperature table
        with self.engine.connect() as connection:
            for index, row in self.data.iterrows():
                session = connection.begin()
                stmt = sqlalchemy.insert(self.temperature).values(
                    place=row["place_code"],
                    date=f"{row['year']}-{row['month']}-{row['day']}",
                    lowest=None if row["lowest_temperature"] == "NULL" else row["lowest_temperature"],
                    highest=None if row["highest_temperature"] == "NULL" else row["highest_temperature"],
                )
                connection.execute(stmt)
                session.commit()

    # QUERIES:
    # 1. Find the number of snowy days on each location. Which location (name) has had most snowy
    # days? For this location, find the month with most snow (sum). For the location with least snowy
    # days, find the month with most snowy days.
    def query_01(self): 
        print("------------------------ Query 1 ------------------------")

        # Query to find the location with the most snowy days
        query = text("""
        SELECT name, COUNT(snow) AS snowy_days
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE snow > 0
        GROUP BY name
        ORDER BY snowy_days DESC
        LIMIT 1;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Location with most snowy days: ")
        print(df)

        # Query to find the month with most snow for the location with most snowy days
        query = text("""
        SELECT name, extract(month from date) as month, SUM(snow) AS total_snow
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE snow > 0 AND name = :name
        GROUP BY name, month
        ORDER BY total_snow DESC
        LIMIT 1;
        """)
        res = self.engine.connect().execute(query, parameters={"name": df["name"][0]})
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Month with most snow for the location with most snowy days: ")
        print(df)

        # Query to find the location with the least snowy days
        query = text("""
        SELECT name, COUNT(snow) AS snowy_days
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE snow > 0
        GROUP BY name
        ORDER BY snowy_days ASC
        LIMIT 1;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Location with least snowy days: ")
        print(df)

        # Query to find the month with most snowy days for the location with least snowy days
        query = text("""
        SELECT name, extract(month from date) as month, COUNT(snow) AS snowy_days
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE snow > 0 AND name = :name
        GROUP BY name, month
        ORDER BY snowy_days DESC
        LIMIT 1;
        """)
        res = self.engine.connect().execute(query, parameters={"name": df["name"][0]})
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Month with most snow for the location with least snowy days: ")
        print(df)