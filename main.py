import csv
import json
import pandas as pd

def count_levels(file_path):
    return file_path.count('/')

def process_list_files(input_filepath, output_filepath, max_level):
    with open(input_filepath, 'r') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                if i >= 50000:
                    break
                strip_line = line.strip()
                split_line = strip_line.split(maxsplit=11)
                pathname = split_line[-1]
                levels = count_levels(pathname)
                max_level = max(max_level, levels)
                writer.writerow(split_line)

def load_data(file_path, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, header=None)
    df.columns = ['number1', 'number2', 'number3', 'permissions', 'owner', 'group', 'size_in_bytes', 'size_in_kb', 'access_time', 'modification_time', '--', 'full_pathname']
    
    # transfer access time to human readable format
    df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
    #df["modification_datetime"] = pd.to_datetime(df['modification_time'], unit='s', origin='unix')

    # stick with only the columns we want
    df = df[['owner', 'size_in_bytes', 'size_in_kb', 'access_datetime', 'full_pathname']]
    
    # create levels of directories and files
    split_path = df['full_pathname'].str.split('/', expand = True).iloc[:, 1:]
    df = pd.concat([split_path, df], axis = 1)

    index_df = df.set_index(df.columns[:20].tolist())
    
    # output the sample file 
    output_file = 'output.csv'
    index_df.to_csv(output_file, index=True)
    
    return index_df

def main():
    with open('config.json') as config_file:
        config = json.load(config_file)
        old_file_path = config["old_file_path"]
        new_file_path = config["new_file_path"]
        max_level = 0
        process_list_files(old_file_path, new_file_path, max_level)
        index_df = load_data(new_file_path)
        test_value = index_df.loc[('gpfs4', 'projectnb', 'econdept', 'lilymar', 'MetricsIvan', 'QTEwc.png'), 'size_in_bytes']
        print(test_value)

if __name__ == "__main__":
    main()
