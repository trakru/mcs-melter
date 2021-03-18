#load libraries
import pandas as pd
import glob
from pathlib import Path

# Provide local file path
RAW_FILE_FOLDER = './data/raw/'
FINAL_FILE_FOLDER = './data/final/'
RADIOSTATS_FILE = glob.glob(RAW_FILE_FOLDER + '*.csv')

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