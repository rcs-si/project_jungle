import csv
import json
import pandas as pd

def copy_lines(old_file_path, new_file_path, num_lines=10000):
    with open(old_file_path, 'r') as infile:
        with open(new_file_path, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                if i >= num_lines:
                    break
                strip_line = line.strip()
                split_line = strip_line.split(maxsplit=11)
                writer.writerow(split_line)

def load_data(new_file_path, delimiter=','):
    df = pd.read_csv(new_file_path, delimiter=delimiter, header=None)
    df.columns = ['number1', 'number2', 'number3', 'permissions', 'owner', 'group', 'size_in_bytes', 'size_in_kb', 'access_time', 'modification_time', '--', 'full_pathname']

    df["access_datetime"] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
    df["modification_datetime"] = pd.to_datetime(df['modification_time'], unit='s', origin='unix')

    split_path = df['full_pathname'].str.split('/').tolist()

    for index, split_path in enumerate(zip(*split_path)):
        if index < 10:
            df[f"path_part_{index}"] = split_path

    return df

def main():
    with open('config.json') as config_file:
        config = json.load(config_file)
        old_file_path = config["old_file_path"]
        new_file_path = config["new_file_path"]
        copy_lines(old_file_path, new_file_path)

if __name__ == "__main__":
    main()
