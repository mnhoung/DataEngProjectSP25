import psycopg2
import json
from datetime import datetime, timedelta
import pandas as pd


def db_connect():
    DBNAME = 'postgres'
    DBUSR = 'postgres'
    DBPASS = 'asdf1234'
    connection = psycopg2.connect(
        host="localhost",
        database=DBNAME,
        user=DBUSR,
        password=DBPASS,
	)
	connection.autocommit = True
	return connection

def read_data(file_name):
    data_list = []
    with open(file_name, 'r') as file:
        for line in file:
            data = json.loads(line.strip())
            data_list.append(data)
    return data_list

def transform_data(data):
    df = pd.DataFrame(data)
    df = df.sort_values(by=['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'ACT_TIME'])
    df['OPD_DATE'] = datetime.strptime(df['OPD_DATE'], '%Y-%m-%d')
    df['ACT_TIME'] = timedelta(seconds=int(df['ACT_TIME']))

    # caluclate the speed
    df['PREV_METERS'] = df.groupby(['EVENT_NO_TRIP', 'EVENT_NO_STOP'])['METERS'].shift(1)
    df['PREV_ACT_TIME'] = df.groupby(['EVENT_NO_TRIP', 'EVENT_NO_STOP'])['ACT_TIME'].shift(1)
    df['SPEED'] = (df['METERS'] - df['PREV_METERS']) / (df['ACT_TIME'] - df['PREV_ACT_TIME'])
    # for every bus on the same trip, fil in the first breadcrumb with the second
    df['SPEED'] = df.groupby(['EVENT_NO_TRIP', 'EVENT_NO_STOP'])['SPEED'].apply(lambda x: x.fillna(method='bfill', limit=1))

def get_sql_cmd(data_list):
    cmd_list = []
    for row in data_list:
        valstr = 

def load_to_db(conn, cmd_list):
    DATA_TABLE_NAME = 'breadcrumb'
    TRIP_TABLE_NAME = 'trip'
    transformed_df = transform_data(df)
    with conn.cursor() as cursor:
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
        for cmd in cmd_list:


        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

