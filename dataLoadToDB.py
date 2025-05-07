import psycopg2
import json
from datetime import datetime, timedelta
import pandas as pd

'''
DBNAME = 'postgres'
DBUSR = 'postgres'
DBPASS = 'asdf1234'
DATA_TABLE_NAME = 'breadcrumb'
TRIP_TABLE_NAME = 'trip'
'''

class LoadToDB:
    def __init__(self):
        self.dbname = 'postgres'
        self.dbuser = 'postgres'
        self.dfpass = 'asdf1234'
        self.breadcrumb_table_name = 'breadcrumb'
        self.trip_table_name = 'trip'
        self.conn = None

    def db_connect():
        self.conn = psycopg2.connect(
            host="localhost",
            database=self.dbname,
            user=self.dbuser,
            password=self.dbpass,
        )
        self.conn.autocommit = True
        return self.conn

    # for if we miss data
    def read_data(file_name):
        data_list = []
        with open(file_name, 'r') as file:
            for line in file:
                data = json.loads(line.strip())
                data_list.append(data)
        return data_list

    def transform_data(data):
        # turn the list to a pandas dataframe
        df = pd.DataFrame(data)A

        # sort the list by vehicle id, then trip, then stop then the act time
        df = df.sort_values(by=['VEHICLE_ID', 'EVENT_NO_TRIP', 'EVENT_NO_STOP', 'ACT_TIME'])

        # transform opd_date and act_time into a timestamp
        df['OPD_DATE'] = datetime.strptime(df['OPD_DATE'], '%d%b%Y:%H:%M:%S')
        df['ACT_TIME'] = timedelta(seconds=int(df['ACT_TIME']))

        # caluclate the speed
        df['PREV_METERS'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['METERS'].shift(1)
        df['PREV_ACT_TIME'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['ACT_TIME'].shift(1)
        df['SPEED'] = (df['METERS'] - df['PREV_METERS']) / (df['ACT_TIME'] - df['PREV_ACT_TIME'])
        # for every bus on the same trip, fil in the first breadcrumb with the second
        df['SPEED'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['SPEED'].apply(lambda x: x.fillna(method='bfill', limit=1))

    def load_to_db(data_list):
        with self.conn.cursor() as cursor:
            print(f"Loading {len(sql_cmd_list)} rows")
            start = time.perf_counter()
            
            '''
            for data in cmd_list:
                breadcrumb_sql = f"INSERT INTO {DATA_TABLE_NAME} (tstamp, latitude, longitude, speed, trip_id)
                VALUES ({%s, %s, %s, %s, %s);"
                trip_sql = f"INSERT INTO {TRIP_TABLE_NAME} (trip_id, route_id, vehicle_id, service_key, direction)
                VALUES ({%s, %s, %s, %s, %s);"
                cursor.execute(breadcrumb_sql, )
                cursor.execute(trip_sql, )
            '''


            elapsed = time.perf_counter() - start
            print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    def run(data_list):
        # validate data NOT IMPLEMENTED YET
        #validate_data = validate_data(data_list)
        # transform data HALF IMPLEMENTED
        transformed_data = transform_data(validate_data)
        # send to database
        load_to_db(transformed_data)
