import argparse
import csv
import json
from pathlib import Path
import pandas as pd
import os
from analyze import analyze_data
from timeit import default_timer as timer

from multiprocessing import Pool

def timer_func(func):
    def wrapper(*args, **kwargs):
        t1 = timer()
        result = func(*args, **kwargs)
        t2 = timer()
        print(f'{func.__name__}() executed in {(t2-t1):.6f}s')
        return result
    return wrapper

def count_levels(file_path):
    return file_path.count('/')

def process_list_files(input_filepath, output_filepath):
    max_level = 0
    index_error_raise_count = 0
    other_error = 0
    with open(input_filepath, 'r') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            try:
                for i, line in enumerate(infile):
                    strip_line = line.strip()
                    split_line = strip_line.split(maxsplit=11)
                    pathname = split_line[-1]
                    levels = count_levels(pathname)
                    max_level = max(max_level, levels)
                    owner = split_line[4]
                    size_in_bytes = split_line[6]
                    size_in_kb = split_line[7]
                    access_time = split_line[8]
                    full_pathname = split_line[11]
                    writer.writerow([owner, size_in_bytes, size_in_kb, access_time, full_pathname])
            except UnicodeDecodeError:
                index_error_raise_count += 1
            except Exception as e:
                other_error += 1

    return max_level, index_error_raise_count, other_error

def load_data(file_path, max_level, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, header=None)
    df.columns = ['owner', 'size_in_bytes', 'size_in_kb', 'access_time', 'full_pathname']
    df['size_in_gb'] = df['size_in_bytes'] / 1e9

    # transfer access time to human readable format
    df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
    df = df[['owner', 'size_in_gb', 'access_datetime', 'full_pathname']]
    
    # create levels of directories and files
    split_path = df['full_pathname'].str.split('/', expand = True).iloc[:, 1:]
    df = pd.concat([split_path, df], axis = 1)

    index_df = df.set_index(df.columns[:max_level].tolist())
    return index_df


@timer_func
def main():
    parser = argparse.ArgumentParser(description='take input file')
    parser.add_argument("-f", "--file")
    args = parser.parse_args()
    with open('config.json') as config_file:
        config = json.load(config_file)
        old_file_dir = config["file_path"]["input_file_dir"]
        new_file_dir = config["file_path"]["output_file_dir"]
        file_path = Path(old_file_dir + '/' + args.file)
        if not file_path.exists():
            print(file_path)
            print("The target file doesn't exist")
            raise SystemExit(1)
    
    
        max_level, index_error_raise_count, other_error = process_list_files(old_file_dir + '/' + args.file, new_file_dir + "/new_" + args.file)
        print("Index error raised: ", index_error_raise_count)
        print("Other error raised: ", other_error)
        print("-------------------- Finished preprocessing --------------------")

        current_datetime = pd.Timestamp.now()
        years_ago = current_datetime - pd.Timedelta(days=365*config["analysis_parameter"]["years"])
        levels = config["analysis_parameter"]["levels"]
        gb_threshold = config["analysis_parameter"]["gb_threshold"]

        #nslots = int(os.getenv("NSLOTS"))

        index_df = load_data(new_file_dir + "/new_" + args.file, max_level)
        print("-------------------- Finished loading data --------------------")
        # TODO add multiprocess
        final_df = analyze_data(index_df, levels, gb_threshold, years_ago)
        print("-------------------- Finished analyzing data --------------------")
        final_df.to_csv("final_df.csv")

if __name__ == "__main__":
    main()
