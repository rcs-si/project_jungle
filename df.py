import pandas as pd

def load_data(filename, delimiter=','):
    df = pd.read_csv(filename, delimiter=delimiter, header=None)
    df.columns = ['number1', 'number2', 'number3', 'permissions', 'owner', 'group', 'size_in_bytes', 'size_in_kb', 'access_time', 'modification_time', '--', 'full_pathname']
    return df

