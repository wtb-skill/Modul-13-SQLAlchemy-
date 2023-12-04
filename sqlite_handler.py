from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData, ForeignKeyConstraint


class SQLiteHandler:
    def __init__(self, db_name='stations.db'):
        self.engine = self.create_engine(db_name)
        self.metadata = MetaData()

    def create_engine(self, db_name):
        return create_engine(f'sqlite:///{db_name}')

    def create_tables(self, stations_data, measures_data):
        self.create_stations_table(stations_data)
        self.create_measures_table(measures_data)

    def create_stations_table(self, data):
        table_name = 'stations'
        columns = {
            'station': String,
            'latitude': Float,
            'longitude': Float,
            'elevation': Float,
            'name': String,
            'country': String,
            'state': String
        }

        table_columns = [
            Column(col_name, col_type(), primary_key=(col_name == 'station'))
            for col_name, col_type in columns.items()
        ]
        table = Table(table_name, self.metadata, *table_columns)
        self.metadata.create_all(self.engine)

        if data is not None:
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def create_measures_table(self, data):
        table_name = 'measures'
        columns = {
            'measurement_id': Integer,
            'station': String,
            'date': Date,
            'precip': Float,
            'tobs': Float
        }

        table_columns = [
            Column(col_name, col_type(), primary_key=(col_name == 'measurement_id'))
            for col_name, col_type in columns.items()
        ]
        table = Table(table_name, self.metadata, *table_columns, sqlite_autoincrement=True)

        # Create a foreign key constraint
        fk_constraint = ForeignKeyConstraint(['station'], ['stations.station'])
        table.append_constraint(fk_constraint)  # Add the constraint to the table

        self.metadata.create_all(self.engine)

        if data is not None:
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)

