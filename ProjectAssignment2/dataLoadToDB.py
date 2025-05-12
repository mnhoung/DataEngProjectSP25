import psycopg2
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

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
        self.dbpass = 'asdf1234'
        self.breadcrumb_table_name = 'breadcrumb'
        self.trip_table_name = 'trip'
        self.conn = None


    def db_connect(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database=self.dbname,
            user=self.dbuser,
            password=self.dbpass,
        )
        self.conn.autocommit = True
    
    
    def validate_data(self, df):
        df = df.copy()
        invalid_indices = set()
        
        # Existence - check required fields are not null
        required_cols = ['VEHICLE_ID', 'EVENT_NO_TRIP', 'ACT_TIME', 'OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE']
        for col in required_cols:
            df = df[df[col].notnull()]

        # Limit - convert fields to numbers
        df['GPS_LATITUDE'] = pd.to_numeric(df['GPS_LATITUDE'], errors='coerce')
        df['GPS_LONGITUDE'] = pd.to_numeric(df['GPS_LONGITUDE'], errors='coerce')
        df['METERS'] = pd.to_numeric(df['METERS'], errors='coerce')
        df['GPS_SATELLITES'] = pd.to_numeric(df['GPS_SATELLITES'], errors='coerce')
        df['ACT_TIME'] = pd.to_numeric(df['ACT_TIME'], errors='coerce')
        df['GPS_HDOP'] = pd.to_numeric(df['GPS_HDOP'], errors='coerce')
        invalid_indices.update(df[(df['GPS_LATITUDE'] < 45) | (df['GPS_LATITUDE'] > 46)].index)
        invalid_indices.update(df[(df['GPS_LONGITUDE'] < -123) | (df['GPS_LONGITUDE'] > -122)].index)
        invalid_indices.update(df[(df['GPS_SATELLITES'] < 3) | (df['GPS_SATELLITES'] > 31)].index)
        invalid_indices.update(df[(df['ACT_TIME'] > 86400)].index)

        # Intra-record - cannot compute HDOP with knowing number of satellites
        invalid_indices.update(df[(df['GPS_SATELLITES'].isna()) & (df['GPS_HDOP'].notna())].index)
        
        # Referential integrity - remove all rows that share the same timestamp for the same vehicle and date 
        duplicates = df.duplicated(subset=['VEHICLE_ID', 'OPD_DATE', 'ACT_TIME'], keep=False)
        invalid_indices.update(df[duplicates].index)

        # Drop invalid rows
        if invalid_indices:
            print(f"Dropping {len(invalid_indices)} invalid rows during validation.")
            df = df.drop(index=invalid_indices)
            
        # Statistical
        mean_hdop = df['GPS_HDOP'].mean()
        if mean_hdop > 5:
            print(f"Warning: High average GPS_HDOP: {mean_hdop:.2f}")

        return df


    def transform_data(self, df):
        df = df.copy()
        
        # transform opd_date and act_time into a timestamp
        df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')
        df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='s')
        df['tstamp'] = df['OPD_DATE'] + df['ACT_TIME']
        
        df = df.sort_values(by=['VEHICLE_ID', 'EVENT_NO_TRIP', 'tstamp'])
        
        # Inter-record validation - time should increase within a trip
        invalid_indices = set()
        for (vehicle_id, trip_id), group in df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP']):
            timestamps = group['tstamp'].values
            for i in range(1, len(timestamps)):
                if timestamps[i] <= timestamps[i - 1]:
                    invalid_indices.add(group.index[i])

        if invalid_indices:
            df = df.drop(index=invalid_indices)

        # calculate the speed
        df['PREV_METERS'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['METERS'].shift(1)
        df['PREV_TIME'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['tstamp'].shift(1)
        df['DELTA_METERS'] = df['METERS'] - df['PREV_METERS']
        df['DELTA_SECONDS'] = (df['tstamp'] - df['PREV_TIME']).dt.total_seconds()
        df['SPEED'] = df['DELTA_METERS'] / df['DELTA_SECONDS']
        
        # backfill the first breadcrumb
        df['SPEED'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['SPEED'].bfill(limit=1)
        return df
    

    def load_to_db(self, df):
        self.db_connect()
        cursor = self.conn.cursor()
        
        # trip table
        trip_records = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].drop_duplicates()

        trip_insert_query = f"""
            INSERT INTO {self.trip_table_name} (trip_id, route_id, vehicle_id, service_key, direction)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
        """
        for _, row in trip_records.iterrows():
            cursor.execute(trip_insert_query, (
                int(row['EVENT_NO_TRIP']), None, int(row['VEHICLE_ID']), None, None))

        # breadcrumb table
        breadcrumb_insert_query = f"""
            INSERT INTO {self.breadcrumb_table_name}
            (tstamp, latitude, longitude, speed, trip_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            cursor.execute(breadcrumb_insert_query, (
                row['tstamp'].to_pydatetime(), float(row['GPS_LATITUDE']), float(row['GPS_LONGITUDE']), float(row['SPEED']), int(row['EVENT_NO_TRIP'])))
        cursor.close()
        
        print(datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d'))
        print(f'Loaded {len(trip_records)} records to trip table')
        print(f'Loaded {len(df)} records to breadcrumb table')


    def run(self, df):
        validated_df = self.validate_data(df)
        transformed_df = self.transform_data(validated_df)
        self.load_to_db(transformed_df)
