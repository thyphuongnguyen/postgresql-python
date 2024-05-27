import pandas

# Sanitize the data as following:
# - Observation: rain and snow should have value 0, if there is no rain or snow. If the rain/snow
# observation is missing, value should be NULL.
# - If any observation value is missing, the value should be NULL.
# - Temperature: even the minimum/maximum value is measured between previous day 8 pm
# and current day 8 pm, it can be treated as minimum/maximum for the day the day column
# determines.


# Load the data from the CSV files
def load_data(file_path) -> pandas.DataFrame:
    try:
        weather_data = pandas.read_csv(file_path)
        print(weather_data.head())

        # Replace missing values with NULL
        weather_data = weather_data.fillna("NULL")

        # Replace -1 in rain and snow with 0
        weather_data["rain"] = weather_data["rain"].replace(-1, 0)
        weather_data["snow"] = weather_data["snow"].replace(-1, 0)

        # Get all records with time = 06:00
        time_06 = weather_data[weather_data["time"] == "06:00"]
        print(time_06)

        # Assign ground temperature to that day
        for index, row in time_06.iterrows():
            day = row["day"]
            month = row["month"]
            year = row["year"]
            place = row["place"]
            ground_temperature = row["ground_temperature"]
            weather_data.loc[
                (weather_data["day"] == day)
                & (weather_data["month"] == month)
                & (weather_data["year"] == year)
                & (weather_data["place"] == place),
                "ground_temperature",
            ] = ground_temperature

        # Drop the records with time = 06:00
        weather_data = weather_data[weather_data["time"] != "06:00"]

        # export the data to a new CSV file
        weather_data.to_csv("weather_data_sanitized.csv", index=False)

        print(weather_data)
        return weather_data
    except Exception as e:
        print(e)
        print("Data loading failed")
