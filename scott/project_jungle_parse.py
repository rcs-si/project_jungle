# This script parses a preprocessed .csv file from an original .list file
# The CSV file is stored in a pandas dataframe
# The final dataframe contains a subset of the columns that are needed for further analysis

import pandas as pd
import os

filepath = "/projectnb/scv/saladenh/project_jungle/projectnb_econdept.csv"

column_names = ["number1", 
                "number2", 
                "number3", 
                "permissions",
                "owner", 
                "group", 
                "size_in_bytes", 
                "size_in_kb", 
                "access_time", 
                "modification_time", 
                "dashes", 
                "full_pathname"]


columns_to_drop = ["number1", 
                    "number2", 
                    "number3",  
                    "size_in_kb", 
                    "dashes"]

raw_df = pd.read_csv(filepath, sep=",", names=column_names)
df = raw_df.drop(columns_to_drop, axis=1)

df["access_datetime"]= pd.to_datetime(df.access_time, unit='s', origin='unix')
df["modification_datetime"]= pd.to_datetime(df.modification_time, unit='s', origin='unix')

df["split_path"] = df["full_pathname"].apply(lambda x: os.path.normpath(x).split(os.path.sep)) 

print(df.head())

