import csv
import json
import time
import pandas as pd
import numpy as np
import datetime 
import argparse

#Cadre:
#period                    N `size (GB)`
#10 years or more         15           0
#between 7.5 and 10       29           0
#between 5 and 7.5      7602           9
#between 2.5 and 5  14653281       19271
#less than 2.5      10446739      634217

def gen_categories(bins):
    ''' From the bins, generate categories for each.'''
    cats = []
    if len(bins) < 2:
        raise Exception(f'bins is too short: {len(bins)}')
        
    cats.append(f'less than {bins[0]}') 
    # Loop thru bins[1:-2] to fill in the in between values.
    for i,b in enumerate(bins[1:]):
        cats.append(f'between {bins[i]} and {b}')
    cats.append(f'greater than {bins[-1]}')
    return cats
    
def summarize_file(infile, oldest_years, year_incr):
    # What time is it?
    now =  pd.to_datetime(datetime.datetime.now())
    # TODO: make this switchable, between right now
    # and the time when the infile was created.
    
    # Read in the 2 columns we need using pandas. This is 
    # the fastest way to read this. Columns 7 and 9 hold the
    # size in bytes and the access time in seconds.
    # As the resulting dataframe is not very big this is quick to read.
    # the access_time column is converted on the fly to a datetime.
    
    df = pd.read_csv(infile,usecols=[6,8], names=['size', 'access_time'],  sep='\\s+',
        dtype={'size':float, 'access_time':float}, on_bad_lines='skip',
        encoding_errors='backslashreplace')
    df['size'] = df['size'] / 1e9  # convert bytes to GB
    bins = list(np.arange(year_incr, oldest_years + year_incr, year_incr))  
    # Get the categories for adding a new column for binning by year.
    cats = gen_categories(bins)
    # Strictly speaking this ignores leap years, but that should be ok.
    df['period'] = pd.cut((time.time() - df['access_time'])/(365*24*3600), 
                         bins=[0]+bins+[oldest_years*10], include_lowest=True, labels=cats)
    return df.groupby('period',observed=True).agg({"size": "sum", "access_time": "count"}).\
           rename(columns={'size':'size (GB)','access_time':'N'}) 

 
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Summarize disk usage by age.")
    # argparse will check to see if this file exists and is readable.
    parser.add_argument("-i", "--infile", help="Input file to analyze",
                        type=argparse.FileType('r', encoding='UTF-8', 
                        errors='backslashreplace'), required=True)
    parser.add_argument("-o", "--outfile", help="Input file to analyze")     
    args = parser.parse_args()
    
    infile = args.infile
    outfile = None
    if args.outfile:
        outfile = args.outfile
    
    #with open('config.json') as config_file:
    # TODO: read the year bins from the config file.
    # For now, hard-code: >10 years, 10 to 7.5, 7.5 to 5, 5 to 2.5, less than 2.5
    oldest_years = 10
    year_incr = 2.5
    
    results = summarize_file(infile, oldest_years, year_incr)
    pd.options.display.float_format = '{:,.1f}'.format
    if outfile:
        outfile.write(results)
        outfile.close()
    else:
        print(results)
