# import required libraries

import boto3
import awswrangler as wr
import pandas as pd
import time

# create boto session

session = boto3.session.Session(profile_name='saml')
s3 = session.resource('s3')

# reading parquet files

parquet_bucket = 's3://xwifi-od-s3-parquet/snmp/type=radiostatsMCS/*'
final_processing_bucket = 's3://xwifi-od-s3-testingbkt/type=radiostatsMCS/'
# result = wr.s3.list_objects(parquet_bucket, boto3_session=session)

# temp condition for testing
result = result[0:4]

# create the result list for processing
# to-do: get & sort filename from parquet & final buckets
# processed_results = wr.s3.list_objects(final_processing_bucket, boto3_session=session)
# raw_result = wr.s3.list_objects(parquet_bucket, boto3_session=session)

# Create a column list to select only the numeric columns 
# Defining two sets of helper functions to apply astype & sum functions across selected columns

def column_list_maker(df):
    """creates a list of columns that starts with 2.4 & 5"""
    return tuple(var for var in df.columns if var.startswith(("2.4", "5")))

def column_dict_maker(df):
    """Returns a dictionary of 72 columns 
    that are supposed to have int values"""
    column_dict = {}
    for idx, c in enumerate(column_list_maker(df)):
        column_dict[c]= int
    return column_dict

def agg_dict_maker(df):
    """Returns a dictionary of 72 columns 
    that are supposed to be aggregated into
    sum values"""
    agg_dict = {}
    for idx, c in enumerate(column_list_maker(df)):
        agg_dict[c]= sum
    return agg_dict

## Pandas manipulation

def s3_read_parquet(item):
    df = wr.s3.read_parquet(item, boto3_session=session)
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
    df = df.drop(['current_ip', 'current_location', 'dt'], axis=1)
    df = df.rename(columns={'current_mac':'snmpreportedmac','timestamp':'date'})
    df = df.fillna(0)
    df = df.astype(column_dict_maker(df))
    df = df.groupby(['date','snmpreportedmac'], as_index=False).agg(agg_dict_maker(df))
    df = pd.melt(df, id_vars=['date', 'snmpreportedmac'], value_vars=column_list_maker(df))
    df['sort_order'] = df['variable'].str.extract(r'[2,5]\.?[4]?_\w{2}_mcs(\d*)')
    df['transmission_type'] = df['variable'].str.extract(r'[2,5]\.?[4]?_(\w{2})_mcs\d*')
    df['radio_type'] = df['variable'].str.extract(r'([2,5]\.?[4]?)_\w{2}_mcs\d*')
    df['dt'] = df['date'].astype(str).str.extract(r'(\d{4}-\d{2})-\d{2}')
    return df


def s3_write_to_parquet(df):
    """loops through all files wr.s3.list_objects
    executes mcs_melter_aws to create dataframes
    saves dataframes as parquet files
    uploads parquet files back to S3"""
    wr.s3.to_parquet(
        df=df,
        path=final_processing_bucket,
        dataset=True,
        boto3_session=session,
        partition_cols=['dt'],
        database='test-table',  # Athena/Glue database
        table='radiostatsMCS_melted_parquet'  # Athena/Glue table
    )

def mcs_melter_aws(item):
    s3_write_to_parquet(s3_read_parquet(item))

# Final execution loop
def s3_melt_parquet():
    for item in result:
        start_time=time.time()
        print(f'processing {item}')
        mcs_melter_aws(item)
        print(f'Processing complete in {time.time()-start_time} seconds')

if __name__=='__main__':
    s3_melt_parquet()