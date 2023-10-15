from df import load_data

def groupby(df):
    result = df.groupby('path_part_4')['size_in_kb'].sum().reset_index()
    return result

df = load_data('/projectnb/rcs-intern/alana/objects/parsed.csv')
summed_sizes = groupby(df)
print(summed_sizes)
