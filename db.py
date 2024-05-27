import sqlalchemy
import pandas as pd
from sqlalchemy.sql import text
import matplotlib.pyplot as plt

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

    # 2. Inspect the rows in Temperature where both ”highest” and ”lowest” are not NULL. Calculate
    # the sample correlation coefficient between these two attributes. What can you interpret from this
    # value? Find the correlations when grouping by location.
    def query_02(self):
        print("------------------------ Query 2 ------------------------")

        # Query to calculate the sample correlation coefficient between the highest and lowest temperatures
        query = text("""
        SELECT corr(lowest, highest) AS correlation
        FROM temperature
        WHERE lowest IS NOT NULL AND highest IS NOT NULL;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Correlation coefficient between lowest and highest temperatures: ")
        print(df)

        # Query to find the correlations when grouping by location
        query = text("""
        SELECT place.name, corr(lowest, highest) AS correlation
        FROM temperature
        JOIN place ON temperature.place = place.code
        WHERE lowest IS NOT NULL AND highest IS NOT NULL
        GROUP BY place.name;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Correlation coefficient between lowest and highest temperatures grouped by location: ")
        print(df)
    
    # 3. Find out the correlation between average temperature and latitude of the location.
    def query_03(self):
        print("------------------------ Query 3 ------------------------")

        # Query to find the correlation between average temperature and latitude of the location
        query = text("""
        SELECT place.name, corr(air_temperature, latitude) AS correlation
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE air_temperature IS NOT NULL AND latitude IS NOT NULL
        GROUP BY place.name;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Correlation between average temperature and latitude of the location: ")
        print(df)

    # 4. For each location, use myplotlib to plot the number of rainy days for each month as a bar plot.
    def query_04(self):
        print("------------------------ Query 4 ------------------------")

        # Query to find the number of rainy days for each month for each location
        query = text("""
        SELECT place.name, extract(month from date) as month, COUNT(rain) AS rainy_days
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE rain > 0
        GROUP BY place.name, month
        ORDER BY place.name, month;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Number of rainy days for each month for each location: ")
        print(df)

        # Plot the number of rainy days for each month for each location
        for name, group in df.groupby("name"):
            plt.figure()
            plt.bar(group["month"], group["rainy_days"])
            plt.xlabel("Month")
            plt.ylabel("Rainy Days")
            plt.title(f"Number of Rainy Days for each Month at {name}")
            plt.savefig(f"{name}_rainy_days.png")
            plt.show()

    # 5. For each location, plot the average temperature throughout the year. You may plot all the graphs
    # into the same Figure.
    def query_05(self):
        print("------------------------ Query 5 ------------------------")

        # Query to find the average temperature throughout the year for each location
        query = text("""
        SELECT place.name, extract(month from date) as month, AVG(air_temperature) AS avg_temperature
        FROM observation
        JOIN place ON observation.place = place.code
        WHERE air_temperature IS NOT NULL
        GROUP BY place.name, month
        ORDER BY place.name, month;
        """)
        res = self.engine.connect().execute(query)
        # Convert to dataframe
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        print("Average temperature throughout the year for each location: ")
        print(df)

        # Plot the average temperature throughout the year for each location, plot all the graphs into the same figure
        fig, ax = plt.subplots()
        for name, group in df.groupby("name"):
            ax.plot(group["month"], group["avg_temperature"], label=name)
        ax.legend()
        plt.xlabel("Month")
        plt.ylabel("Average Temperature")
        plt.title("Average Temperature throughout the Year for each Location")
        plt.savefig("average_temperature.png")
        plt.show()
