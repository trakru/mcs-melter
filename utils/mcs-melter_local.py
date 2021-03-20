"""
WARNING: This code is old and needs to be migrated to reflect new updates.
DO NOT USE
"""


#load libraries
import awswrangler as wr
import pandas as pd
import glob
from pathlib import Path
import boto3
from saml_util import samlapi_formauth
import os

#go up a directory
os.chdir(Path().resolve().parents[0])

p = Path('data','raw')
# final_path = Path('data','final','testfile.csv')
RADIOSTATS_FILE = [file for file in p.glob('*.csv')]

# Provide local file path
RAW_FILE_FOLDER = './data/raw/'
FINAL_FILE_FOLDER = './data/final/'
# RADIOSTATS_FILE = glob.glob(RAW_FILE_FOLDER + '*.csv')

# provide s3 Path & file locations
# session = boto3.session.Session(profile_name='saml')
# s3 = session.resource('s3')


# reading parquet files

# parquet_bucket = 's3://xwifi-od-s3-parquet/snmp/type=radiostatsMCS/*'
# result = wr.s3.list_objects(parquet_bucket, boto3_session=session)

# # initialize dataframe
# df = wr.s3.read_parquet(result[0], boto3_session=session)

# my_bucket = s3.Bucket('some/path/')

# for my_bucket_object in my_bucket.objects.all():
#     print(my_bucket_object)

# mybucket.objects.filter(Prefix='foo/bar')

# Read csv and provide manipulations
df = pd.read_csv(RADIOSTATS_FILE[0])

# Create column dictionary for dtype assertion
column_dict = {}
for idx, c in enumerate(df.columns):
    if idx <=3:
        continue
    else:
        column_dict[c]= int

# Dataframe operations

df = df.fillna(0)
df = df.astype(column_dict)
df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.date
df = df.drop(['Current_IP', 'Current_Location'], axis=1)
df = df.rename(columns={'Current_MAC':'snmpreportedmac',
                       'Timestamp':'Date'})


# Create the aggregate dictionary
agg_dict = {}
for idx, c in enumerate(df.columns):
    if idx <=1:
        continue
    else:
        agg_dict[c]= sum

# Basic aggregation & melting

df = df.groupby(['Date','snmpreportedmac'], as_index=False).agg(agg_dict)
column_list = tuple(var for var in df.columns if var not in ['Date', 'snmpreportedmac'])
df = pd.melt(df, id_vars=['Date', 'snmpreportedmac'], value_vars=column_list) 

# regex to create sort_order, tranmission_type & radio_type columns

df['sort_order'] = df['variable'].str.extract(r'[2,5]\.?[4]?_\w{2}_mcs(\d*)')
df['transmission_type'] = df['variable'].str.extract(r'[2,5]\.?[4]?_(\w{2})_mcs\d*')
df['radio_type'] = df['variable'].str.extract(r'([2,5]\.?[4]?)_\w{2}_mcs\d*')

# write final file
df.to_csv(
    FINAL_FILE_FOLDER + 
    'melted_' + 
    RADIOSTATS_FILE[0][len(RAW_FILE_FOLDER):]
    )