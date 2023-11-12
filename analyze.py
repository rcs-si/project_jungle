import pandas as pd

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

new_file_path = '/projectnb/rcs-intern/project_jungle/pp_results.list'

df = load_data(new_file_path)

df.to_csv('/projectnb/rcs-intern/project_jungle/full_dataframe.csv')

summed_sizes = df.groupby('path_part_4')['size_in_kb'].sum().reset_index()

summed_sizes.to_csv('/projectnb/rcs-intern/project_jungle/a_results.csv')
