import psycopg2
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv()

class LoadToDB:
    def __init__(self):
        self.dbname = 'postgres'
        self.dbuser = 'postgres'
        self.dbpass = os.getenv("DBPASS")
        self.breadcrumb_table_name = 'breadcrumb'
        self.conn = None

    def db_connect(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database=self.dbname,
            user=self.dbuser,
            password=self.dbpass,
        )
        self.conn.autocommit = True
    
    # Existence (1)
    def assert_required_fields_not_null(self, df):
        try:
            required_cols = ['VEHICLE_ID', 'EVENT_NO_TRIP', 'ACT_TIME', 'OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE']
            for col in required_cols:
                assert df[col].notnull().all(), f"{col} has null values"
        except AssertionError as e:
            print(f"Required Fields Assertion Error: {e}")
            invalid = df[df[required_cols].isnull().any(axis=1)].index
            return invalid
        return []

    # Limit (2-7)
    def assert_latitude_limits(self, df):
        try:
            assert df['GPS_LATITUDE'].between(45, 46).all(), "Latitude out of bounds"
        except AssertionError as e:
            print(f"Latitude Assertion Error: {e}")
            return df[~df['GPS_LATITUDE'].between(45, 46)].index
        return []

    def assert_longitude_limits(self, df):
        try:
            assert df['GPS_LONGITUDE'].between(-123, -122).all(), "Longitude out of bounds"
        except AssertionError as e:
            print(f"Longitude Assertion Error: {e}")
            return df[~df['GPS_LONGITUDE'].between(-123, -122)].index
        return []
    
    def assert_meters_nonnegative(self, df):
        try:
            assert (df['METERS'] >= 0).all(), "Negative meter readings"
        except AssertionError as e:
            print(f"Meters Assertion Error: {e}")
            return df[df['METERS'] < 0].index
        return []

    def assert_satellites_range(self, df):
        try:
            assert df['GPS_SATELLITES'].between(3, 31).all(), "GPS satellites out of valid range"
        except AssertionError as e:
            print(f"Satellites Assertion Error: {e}")
            return df[~df['GPS_SATELLITES'].between(3, 31)].index
        return []

    def assert_act_time_valid(self, df):
        try:
            assert (df['ACT_TIME'] <= 86400).all(), "ACT_TIME exceeds 86400 seconds"
        except AssertionError as e:
            print(f"ACT_TIME Assertion Error: {e}")
            return df[df['ACT_TIME'] > 86400].index
        return []
    
    def assert_speed_limit(self, df):
        try:
            assert (df['SPEED'] <= 40).all(), "Speed exceeds 40 m/s"
        except AssertionError as e:
            print(f"Speed Assertion Error: {e}")
            return df[df['SPEED'] > 40].index
        return []

    # Intra-record (8)
    def assert_hdop_needs_satellites(self, df):
        try:
            assert not ((df['GPS_SATELLITES'].isna()) & (df['GPS_HDOP'].notna())).any(), "HDOP present without satellite count"
        except AssertionError as e:
            print(f"HDOP Assertion Error: {e}")
            return df[(df['GPS_SATELLITES'].isna()) & (df['GPS_HDOP'].notna())].index
        return []

    # Referential integrity (9)
    def assert_unique_vehicle_time(self, df):
        try:
            dupes = df.duplicated(subset=['VEHICLE_ID', 'OPD_DATE', 'ACT_TIME'], keep=False)
            assert not dupes.any(), "Duplicate timestamps for vehicle on same date"
        except AssertionError as e:
            print(f"Duplicate Timestamp Assertion Error: {e}")
            return df[dupes].index
        return []

    # Inter-record (10)
    def assert_timestamp_monotonic(self, df):
        try:
            invalid = set()
            for (vehicle_id, trip_id), group in df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP']):
                timestamps = group.sort_values(by='tstamp')['tstamp'].values
                for i in range(1, len(timestamps)):
                    if timestamps[i] <= timestamps[i - 1]:
                        invalid.add(group.index[i])
            assert not invalid, "Non-increasing timestamps within trip"
        except AssertionError as e:
            print(f"Timestamp Assertion Error: {e}")
            return list(invalid)
        return []


    def validate_data(self, df):
        df = df.copy()
        invalid_indices = set()

        df['GPS_LATITUDE'] = pd.to_numeric(df['GPS_LATITUDE'], errors='coerce')
        df['GPS_LONGITUDE'] = pd.to_numeric(df['GPS_LONGITUDE'], errors='coerce')
        df['METERS'] = pd.to_numeric(df['METERS'], errors='coerce')
        df['GPS_SATELLITES'] = pd.to_numeric(df['GPS_SATELLITES'], errors='coerce')
        df['ACT_TIME'] = pd.to_numeric(df['ACT_TIME'], errors='coerce')
        df['GPS_HDOP'] = pd.to_numeric(df['GPS_HDOP'], errors='coerce')
        
        invalid_indices = set()
        invalid_indices.update(self.assert_required_fields_not_null(df))
        invalid_indices.update(self.assert_latitude_limits(df))
        invalid_indices.update(self.assert_longitude_limits(df))
        invalid_indices.update(self.assert_meters_nonnegative(df))
        invalid_indices.update(self.assert_satellites_range(df))
        invalid_indices.update(self.assert_act_time_valid(df))
        invalid_indices.update(self.assert_hdop_needs_satellites(df))
        invalid_indices.update(self.assert_unique_vehicle_time(df))
        
        # Timestamp and Speed validations come after transformation
        df = self.transform_data(df)
        invalid_indices.update(self.assert_timestamp_monotonic(df))
        invalid_indices.update(self.assert_speed_limit(df))
        
        if invalid_indices:
            print(f"Dropping {len(invalid_indices)} invalid rows during validation.")
            df = df.drop(index=invalid_indices)

        return df


    def transform_data(self, df):
        df = df.copy()
        
        # transform opd_date and act_time into a timestamp
        df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
        df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='s', errors='coerce')
        df['tstamp'] = df['OPD_DATE'] + df['ACT_TIME']
        
        df = df.sort_values(by=['VEHICLE_ID', 'EVENT_NO_TRIP', 'tstamp'])

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

        breadcrumb_csv = io.StringIO()
        df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].to_csv(
            breadcrumb_csv, index=False, header=False)
        breadcrumb_csv.seek(0)
        cursor.copy_from(breadcrumb_csv, self.breadcrumb_table_name, sep=",")
        
        cursor.close()

        print(f'Loaded {len(df)} records to breadcrumb table')


    def run(self, df):
        final_df = self.validate_data(df)
        self.load_to_db(final_df)
