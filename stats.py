# import os
# import time
# import csv
# from datetime import datetime

# def count_levels(pathname):
#     return pathname.count('/')

# def generate_report(file_path, output_filepath):
#     # Age bins in years
#     bins = [
#         (0, 2.5),
#         (2.5, 5),
#         (5, 7.5),
#         (10, float('inf'))
#     ]
#     bin_counts = {bin_range: 0 for bin_range in bins}
#     bin_sizes_gb = {bin_range: 0.0 for bin_range in bins}

#     current_time = time.time()
#     index_error_raise_count = 0
#     other_error = 0

#     with open(file_path, 'r') as infile:
#         with open(output_filepath, 'w') as outfile:
#             writer = csv.writer(outfile)
#             writer.writerow(["Owner", "Size (Bytes)", "Size (GB)", "Access Time", "Full Pathname", "Age Bin (Years)"])
#             for line in infile:
#                 try:
#                     strip_line = line.strip()
#                     parts = strip_line.split()
#                     if len(parts) < 12:
#                         continue

#                     size = int(parts[6])  # File size in bytes
#                     mtime = float(parts[8])  # Modification time
#                     pathname = parts[-1]  # Full pathname

#                     # Calculate file age in years
#                     age_in_years = (current_time - mtime) / (365 * 24 * 60 * 60)
#                     size_gb = size / (1024 ** 3)  # Convert size to GB

#                     # Determine which bin the file belongs to
#                     age_bin = None
#                     for bin_range in bins:
#                         if bin_range[0] <= age_in_years < bin_range[1]:
#                             bin_counts[bin_range] += 1
#                             bin_sizes_gb[bin_range] += size_gb
#                             age_bin = f"{bin_range[0]} - {bin_range[1]} years"
#                             break

#                     # Write details to output file
#                     owner = parts[4]
#                     access_time = parts[8]
#                     writer.writerow([owner, size, size_gb, access_time, pathname, age_bin])

#                 except UnicodeDecodeError:
#                     index_error_raise_count += 1
#                 except Exception as e:
#                     other_error += 1

#     # Print the report
#     print("\nFile Age Report:\n")
#     for bin_range in bins:
#         count = bin_counts[bin_range]
#         size_gb = bin_sizes_gb[bin_range]
#         print(f"Files between {bin_range[0]} and {bin_range[1]} years old: {count} files, {size_gb:.2f} GB")
    
#     print(f"\nIndex errors: {index_error_raise_count}")
#     print(f"Other errors: {other_error}")

# if __name__ == "__main__":
#     # uncomment lines as needed for test
#     file_path = "data/projectnb_econdept_subset.list"
#     # file_path = 'data/projectnb_econdept.list'
#     output_filepath = "output/file_age_report.csv"
#     # output_filepath = "output/file_age_report_full.csv"
#     if os.path.exists(file_path):
#         generate_report(file_path, output_filepath)
#     else:
#         print(f"File not found: {file_path}")











import csv
import pandas as pd
import numpy as np
import datetime 
import argparse

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
    df = pd.read_csv(infile, usecols=[4, 7, 9], names=['owner', 'size', 'access_time'], delimiter=' ',
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
        print(results)



    # test_results = summarize_file('project_jungle/data/projectnb_econdept_subset.list', oldest_years, year_incr)
    # print(test_results)
