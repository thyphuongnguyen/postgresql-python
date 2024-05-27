import db
import load_data

# Load the data from the CSV file
weather_data = load_data.load_data("weather_data_2020.csv")

# Initialize the database connection
db_manager = db.DBManager(weather_data)
db_manager.init_db_connection()

# Insert the data into the database
db_manager.insert_place()
db_manager.insert_observation()
db_manager.insert_temperature()

# Queries
db_manager.query_01()
