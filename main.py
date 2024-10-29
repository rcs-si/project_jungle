# import argparse
# import csv
# import json
# import os
# import pandas as pd
# import plotly.express as px
# from analyze import analyze_data
# from timeit import default_timer as timer

# def timer_func(func):
#     def wrapper(*args, **kwargs):
#         t1 = timer()
#         result = func(*args, **kwargs)
#         t2 = timer()
#         print(f'{func.__name__}() executed in {(t2-t1):.6f}s')
#         return result
#     return wrapper

# def count_levels(file_path):
#     return file_path.count('/')

# def process_list_files(input_filepath, output_filepath):
#     max_level = 0
#     index_error_raise_count = 0
#     other_error = 0
#     with open(input_filepath, 'r') as infile:
#         with open(output_filepath, 'w') as outfile:
#             writer = csv.writer(outfile)
#             for i, line in tqdm.tqdm(enumerate(infile)):
#                 try:
#                     line_c += 1
#                     strip_line = line.strip()
#                     split_line = strip_line.split(maxsplit=11)
#                     pathname = split_line[-1]
#                     levels = count_levels(pathname)
#                     max_level = max(max_level, levels)
#                     owner = split_line[4]
#                     size_in_bytes = split_line[6]
#                     size_in_kb = split_line[7]
#                     access_time = split_line[8]
#                     full_pathname = split_line[11]
#                     writer.writerow([owner, size_in_bytes, size_in_kb, access_time, full_pathname])
#                 except UnicodeDecodeError:
#                     index_error_raise_count += 1
#                 except Exception as e:
#                     other_error += 1
#     return max_level, index_error_raise_count, other_error

# def load_data(file_path, max_level, delimiter=','):
#     df = pd.read_csv(file_path, delimiter=delimiter, header=None)
#     df.columns = ['owner', 'size_in_bytes', 'size_in_kb', 'access_time', 'full_pathname']
#     df['size_in_gb'] = df['size_in_bytes'] / 1e9

#     # transfer access time to human readable format
#     df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
#     df = df[['owner', 'size_in_gb', 'access_datetime', 'full_pathname']]
    
#     # create levels of directories and files
#     split_path = df['full_pathname'].str.split('/', expand = True).iloc[:, 1:]
#     df = pd.concat([split_path, df], axis = 1)

#     index_df = df.set_index(df.columns[:max_level].tolist())
#     return index_df


# @timer_func
# def main():
#     parser = argparse.ArgumentParser(prog="Project Jungle",
#                                      description="Analyze project directories")
#     parser.add_argument("-f", "--file", help="Input file to analyze")
#     parser.add_argument("-o", "--output", help="Output directory")
#     args = parser.parse_args()

#     with open('config.json') as config_file:
#         config = json.load(config_file)

#         input_filepath = args.file
#         file = args.file.split("/")[-1]
#         filename = file.split(".")[0]

#         output_dir = args.output
#         pp_dir = output_dir + "/pp/"
#         analysis_dir = output_dir + "/analysis/"
#         vis_dir = output_dir + "/viz/"

#         if not os.path.exists(pp_dir):
#             os.makedirs(pp_dir)
        
#         if not os.path.exists(analysis_dir):
#             os.makedirs(analysis_dir)

#         if not os.path.exists(vis_dir):
#             os.makedirs(vis_dir)

#         if not os.path.exists(input_filepath):
#             print(input_filepath)
#             print("The input filepath doesn't exist")
#             raise SystemExit(1)
    
#         max_level, index_error_raise_count, other_error = process_list_files(input_filepath, pp_dir + file)
#         print("Index error raised: ", index_error_raise_count)
#         print("Other error raised: ", other_error)
#         print("-------------------- Finished preprocessing --------------------")

#         current_datetime = pd.Timestamp.now()
#         years_ago = current_datetime - pd.Timedelta(days=365*config["analysis_parameter"]["years"])
#         levels = config["analysis_parameter"]["levels"]
#         gb_threshold = config["analysis_parameter"]["gb_threshold"]

#         index_df = load_data(pp_dir + file, max_level)
#         print("-------------------- Finished loading data --------------------")

#         final_df = analyze_data(index_df, levels, gb_threshold, years_ago)
#         print("-------------------- Finished analyzing data --------------------")
#         analysis_filepath = analysis_dir + filename + ".csv"
#         final_df.to_csv(analysis_filepath)

#         ### Fix the visualizations
#         vis_df = final_df.copy()
#         vis_df.reset_index(inplace=True)
#         vis_df.fillna("NA", inplace=True)
#         vis_df["year"] = vis_df["access_datetime"].dt.year
#         vis_df["size_in_gb"] = vis_df["size_in_gb"].apply(lambda x: x + 1e-9)

#         fig = px.treemap(vis_df, 
#                          path=vis_df.columns[2:levels], 
#                          values='size_in_gb', 
#                          color='year', 
#                          color_continuous_scale='RdBu', 
#                          range_color=[2012, 2024])
#         fig.update_traces(hovertemplate='labels=%{label}<br>size_in_gb=%{value:.1f}<br>parent=%{parent}<br>id=%{id}<br>year=%{color:4i}<extra></extra>')
#         fig.write_html(vis_dir + filename + ".html")

# if __name__ == "__main__":
#     main()


### main.py
import argparse
import json
import os
import shutil
import glob


# Suppress annoying Dask messages
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client

import pandas as pd
import plotly.express as px
from analyze import analyze_data_new, path_extract
from timeit import default_timer as timer
from tqdm import tqdm

def timer_func(func):
    def wrapper(*args, **kwargs):
        t1 = timer()
        result = func(*args, **kwargs)
        t2 = timer()
        print(f'{func.__name__}() executed in {(t2-t1):.6f}s')
        return result
    return wrapper

@timer_func
def concatenate_parts(parts_dir, ofile, remove_dir=True):
    ''' Find all of the files in parts_dir, concatenate them
    to the file ofile. Optionally remove the
    parts_dir at the end. 
    
    The Dask dataframe writes out the CSV as parts with names 
    like so:
        
        000.part   001.part  002.part  etc.
    
    Only 000.part contains the header row. '''
    try:
        # Sort just to make sure that the 000.part gets handled first.
        filenames = sorted(glob.glob(os.path.join(parts_dir, "*.part")))
        with open(ofile, "wb") as outfile:
            for filename in tqdm(filenames, desc=f'Concatenating parts into {ofile}'):
                with open(filename, "rb") as infile:
                    shutil.copyfileobj(infile, outfile)
        if remove_dir:
            shutil.rmtree(parts_dir)
    except Exception as e:
        print('***** Failed to concatenate file parts into a single file.')
        print(f'Directory: {parts_dir}')
        print(f'CSV file: {ofile}')
        print(f'{e}')

@timer_func
def process_list_files(input_filepath, n_workers):
    def process_line(line):
        # line has the format:
        # ["string with data space separated","full pathname"]
        if len(line) != 2:
            #print(f'EXCEPTION: {line}')
            return '', 0, 0, 0, '', 0
        try:
            # Process most of the line
            split_line = line[0].split()
            owner = split_line[4]
            size_in_bytes = split_line[6]
            size_in_kb = split_line[7]
            access_time = split_line[8]
            # And now the filepath
            full_pathname = line[1]
            levels = full_pathname.count('/')
            return owner, size_in_bytes, size_in_kb, access_time, full_pathname, levels
        except (UnicodeDecodeError, IndexError):
            return '', 0, 0, 0, '', 0
        except Exception:
            return '', 0, 0, 0, '', 0

    # Use the Bag built-in strip and split functions. The split on ' -- ' will return
    # a tuple: (most of the line, full path)
    bag = db.read_text(input_filepath, encoding='utf-8',errors='backslashreplace', blocksize='32M').\
                       str.strip().str.split(' -- ')
    # Re-work each line into a tuple, and remove any lines that did not 
    # decode correctly.
    bag = bag.map(process_line).filter(lambda x: len(x[0]) > 0)
    # Convert to a dataframe
    df = bag.to_dataframe(meta={'owner': str, 'size_in_bytes': int, 'size_in_kb': int, 
                                'access_time': str, 'full_pathname': str, 'levels': int})
    # Now add extra columns.
    df['size_in_gb'] = df['size_in_bytes'].astype(float) / 1e9
    df['access_datetime'] = dd.to_datetime(df['access_time'], unit='s', origin='unix')
    return df
 
   
@timer_func
def setup_vis_df(df, min_level, max_level, current_datetime):
    ''' Add columns to the df that correspond to the levels in the paths.
       Then convert it to Pandas, do a little processing, and return. '''
    # Make columns for each path depth for use with the path=
    # argument in treemap.
    cols = list(range(min_level, max_level + 1))
    for col in cols:
        df[str(col)] = df['levels_pathname'].apply(path_extract, args=(col,True),  meta=('levels_pathname', 'str'))
    # Done, now convert to pandas.
    vis_df = df.compute()
    
    vis_df.reset_index(inplace=True)
   # vis_df.fillna("NA", inplace=True)
    vis_df["year_category"] = pd.cut(vis_df["access_datetime"].dt.year, 
                                      bins=[-float('inf'), current_datetime.year - 10, current_datetime.year - 5, current_datetime.year],
                                      labels=["Older than 10 years", "Older than 5 years", "Less than 5 years"])
    vis_df["size_in_gb"] = vis_df["size_in_gb"].apply(lambda x: x + 1e-9)
    return vis_df
    
    
@timer_func
def main():
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Analyze project directories")
    parser.add_argument("-f", "--file", help="Input file to analyze")
    parser.add_argument("-o", "--output", help="Output directory")
    args = parser.parse_args()
    
    # Read the config file.
    with open('config.json') as config_file:
        config = json.load(config_file)

    input_filepath = args.file
    filename_ext = args.file.split("/")[-1]
    filename = filename_ext.split(".")[0]

    output_dir = args.output
    pp_dir = output_dir + "/pp/"
    analysis_dir = output_dir + "/analysis/"
    vis_dir = output_dir + "/viz/"

    if not os.path.exists(pp_dir):
        os.makedirs(pp_dir)
    
    if not os.path.exists(analysis_dir):
        os.makedirs(analysis_dir)

    if not os.path.exists(vis_dir):
        os.makedirs(vis_dir)

    if not os.path.exists(input_filepath):
        print(input_filepath)
        print("The input filepath doesn't exist")
        raise SystemExit(1)

    index_df = process_list_files(input_filepath, n_cores)

    current_datetime = pd.Timestamp.now()
    years_ago = current_datetime - pd.Timedelta(days=365*config["analysis_parameter"]["years"])
    levels = config["analysis_parameter"]["levels"]
    gb_threshold = config["analysis_parameter"]["gb_threshold"]
     
    # Persist the df to avoid re-loading it from disk.
    index_df = index_df.persist()
    # Write out the CSVs in parallel. This is VERY fast.
    index_df[['owner', 'size_in_bytes', 'size_in_kb', 'access_time', 'full_pathname','levels']].\
        to_csv(os.path.join(pp_dir,filename_ext), single_file=False, 
        index=False, header_first_partition_only=True)
    
    # Combine the parts into 1 CSV
    concatenate_parts(os.path.join(pp_dir,filename_ext),
                      os.path.join(pp_dir,filename_ext) + '.csv')

    print("-------------------- Finished preprocessing --------------------")

    max_levels = index_df['levels'].max().compute()
    if levels > max_levels:
        levels = max_levels
    index_df= analyze_data_new(index_df, levels, gb_threshold, years_ago)
    analysis_filepath = analysis_dir + filename + ".csv"
    index_df.to_csv(os.path.join(analysis_dir,'parts'), single_file=False, index=False)
    # Combine the parts into 1 CSV
    concatenate_parts(os.path.join(analysis_filepath,'parts'), analysis_filepath)
    print("-------------------- Finished analyzing data --------------------")

    ### Visualization
    
    # Convert the Dask dataframe to a Pandas dataframe for visualization.
    # 4 is the starting depth for directories in the analysis.
    vis_df = setup_vis_df(index_df, 4, levels, current_datetime )
    import pickle
    with open('vis.pkl','wb') as f:
        pickle.dump(vis_df, f)
    fig = px.treemap(vis_df, 
                     path=[str(x) for x in range(4,levels+1)], #vis_df.columns[4:levels], 
                     values='size_in_gb', 
                     color='year_category', 
                     color_discrete_sequence=px.colors.qualitative.Set1)
    fig.update_traces(hovertemplate='labels=%{label}<br>size_in_gb=%{value:.1f}<br>parent=%{parent}<br>id=%{id}<br>year_category=%{color}<extra></extra>')
    fig.write_html(vis_dir + filename + ".html")
    


p='/gpfs4/projectnb/econdept/tklee/telecom/demand_estimation/output'
path_extract(p, 8, False)
path_extract(p, 10, True)
path_extract(p, 9, True)
path_extract(p, 8, True)
path_extract(p, 7, True)
path_extract(p, 7, True)

if __name__ == "__main__":
    try: 
        n_cores =int( os.environ.get('NSLOTS',1))
        # Leave 1 core for the main Python process.
        n_cores = max(1, n_cores - 1)
        client = Client(n_workers=n_cores, processes=True)
        print(f'Client dashboard: { client.dashboard_link }')
        main()
    finally:
        # Make sure to shut down dask even in the event of failures.
        client.shutdown()
        client.close()
        
        