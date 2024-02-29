import csv
import json
import pandas as pd

def count_levels(file_path):
    return file_path.count('/')

def process_list_files(input_filepath, output_filepath):
    with open(input_filepath, 'r') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                strip_line = line.strip()
                split_line = strip_line.split(maxsplit=11)
                pathname = split_line[-1]
                print(f"Pathname {i+1}: {pathname}")
                levels = count_levels(pathname)
                split_line.append(levels)
                writer.writerow(split_line)

def load_data(file_path, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, header=None)
    df.columns = ['number1', 'number2', 'number3', 'permissions', 'owner', 'group', 'size_in_bytes', 'size_in_kb', 'access_time', 'modification_time', '--', 'full_pathname']

    df["access_datetime"] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
    df["modification_datetime"] = pd.to_datetime(df['modification_time'], unit='s', origin='unix')

    split_path = df['full_pathname'].str.split('/').tolist()
    print(split_path)

    max_levels = max(len(path) for path in split_path)
    levels = [f'Level {i+1}' for i in range(max_levels)]
    #multi_index = pd.MultiIndex.from_tuples(zip(*split_path), names=levels[:len(split_path[i])])

    #df_multiindex = pd.DataFrame(index=df.index, columns=multi_index)
    #print(df_multiindex)

def main():
    with open('config.json') as config_file:
        config = json.load(config_file)
        old_file_path = config["old_file_path"]
        new_file_path = config["new_file_path"]
        process_list_files(old_file_path, new_file_path)
        #load_data(new_file_path)

if __name__ == "__main__":
    main()
