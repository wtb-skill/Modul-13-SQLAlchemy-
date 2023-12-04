from manage_csv import ManageCSV
from sqlite_handler import SQLiteHandler
from sqlalchemy import create_engine, text
from pathlib import Path


def fill_database():
    clean_measure = ManageCSV(r'CSV files/clean_measure.csv')
    clean_stations = ManageCSV(r'CSV files/clean_stations.csv')

    stations_data = clean_stations.data
    measures_data = clean_measure.data

    sqlite_handler = SQLiteHandler()  # Initialize SQLiteHandler
    sqlite_handler.create_tables(stations_data, measures_data)  # Create tables based on data


def query_stations(limit=5):
    engine = create_engine('sqlite:///stations.db')

    with engine.connect() as conn:
        query = text(f"SELECT * FROM stations LIMIT {limit}")
        result = conn.execute(query).fetchall()
        for item in result:
            print(item)


def query_measurements(station_id):
    engine = create_engine('sqlite:///stations.db')

    with engine.connect() as conn:
        query = text("SELECT * FROM measures WHERE station = :station_id")
        result = conn.execute(query.bindparams(station_id=station_id)).fetchall()
        for item in result:
            print(item)


if __name__ == "__main__":
    db_file = 'stations.db'

    if not Path(db_file).is_file():  # Check if the database file exists
        fill_database()

    query_stations(2)

    station_id = 'USC00519397'  # Replace this with the chosen station ID

    query_measurements(station_id)