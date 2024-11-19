import csv
import pandas as pd
import numpy as np
import datetime 
import argparse
import time


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
    
    # Read in the columns we need using pandas.
    df = pd.read_csv(infile, usecols=[4, 6, 8], names=['owner', 'size', 'access_time'], sep='\\s+',
        dtype={'owner': str, 'size': float, 'access_time': float}, on_bad_lines='skip',
        encoding_errors='backslashreplace')
    df['size'] = df['size'] / 1e9  # convert bytes to GB
    bins = list(np.arange(year_incr, oldest_years + year_incr, year_incr))  
    # Get the categories for adding a new column for binning by year.
    cats = gen_categories(bins)
    # Strictly speaking this ignores leap years, but that should be ok.
    df['period'] = pd.cut((time.time() - df['access_time'])/(365*24*3600), 
                         bins=[0]+bins+[oldest_years*10], include_lowest=True, labels=cats)
    
    # Group by 'owner' and 'period' to get the desired summary output
    summary = df.groupby(['owner', 'period'], observed=True).agg({'size': 'sum'}).unstack(fill_value=0)
    summary.columns = [f'Size-GB-{col[1]}' for col in summary.columns]
    summary.reset_index(inplace=True)
    return summary
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Summarize disk usage by age.")
    # argparse will check to see if this file exists and is readable.
    parser.add_argument("-i", "--infile", help="Input file to analyze",
                        type=argparse.FileType('r', encoding='UTF-8', 
                        errors='backslashreplace'), required=True)
    parser.add_argument("-o", "--outfile", help="Output file to write the summary to")     
    args = parser.parse_args()
    
    infile = args.infile
    outfile = None
    if args.outfile:
        outfile = args.outfile
    
    # Hard-code the bins for now: >10 years, 10 to 7.5, 7.5 to 5, 5 to 2.5, less than 2.5
    oldest_years = 10
    year_incr = 2.5
    
    results = summarize_file(infile, oldest_years, year_incr)
    pd.options.display.float_format = '{:,.1f}'.format
    if outfile:
        results.to_csv(outfile, index=False)
    else:
        pd.set_option('display.max_rows',None)
        pd.set_option('display.max_columns',None)
        pd.set_option('display.max_colwidth',None)
        pd.set_option('display.width',None)
        print(results)


