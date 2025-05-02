import argparse
import json
import os
import pandas as pd
from timeit import default_timer as timer
from datetime import datetime


def timer_func(func):
    def wrapper(*args, **kwargs):
        t1 = timer()
        result = func(*args, **kwargs)
        t2 = timer()
        print(f'{func.__name__}() executed in {(t2 - t1):.6f}s')
        return result
    return wrapper


def count_levels(file_path):
    return file_path.count('/')


def path_extract(full_pathname, levels, level_limit=False):
    if level_limit and full_pathname.count('/') < levels:
        return ''
    return '/'.join(full_pathname.split('/')[0:levels + 1])


def process_list_file(input_filepath):
    try:
        df = pd.read_csv(
            input_filepath,
            usecols=[4, 6, 8, 11],  # 4: owner, 6: size, 8: access time, 11: path
            names=['owner', 'size_in_bytes', 'access_time', 'full_pathname'],
            dtype={'owner': str, 'size': float, 'access_time': float, 'full_pathname': str},
            sep='\\s+',
            on_bad_lines='skip',
            encoding_errors='backslashreplace'
        )
    except Exception as e:
        print(f"Failed to read input file: {e}")
        raise

    df = df.dropna()

    if df.empty:
        raise ValueError("DataFrame is empty after filtering. Check the input file format.")

    df['full_pathname'] = df['full_pathname'].str.replace('/gpfs4', '', regex=False)
    df['size_in_gb'] = df['size_in_bytes'] / 1e9
    df['levels'] = df['full_pathname'].str.count('/')
    df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s')

    max_level = 5
    return df, max_level


def filter_and_aggregate(df, max_levels, gb_threshold, time_threshold):
    df['levels_pathname'] = ''
    df_append = []

    for level in range(3, max_levels + 1):
        df_levels = df[df['levels'] >= level].copy()
        df_levels['levels_pathname'] = df_levels['full_pathname'].apply(path_extract, args=(level,))
        df_levels = df_levels.groupby('levels_pathname').agg({
            'size_in_gb': 'sum',
            'access_datetime': 'min'
        }).reset_index()
        df_levels = df_levels.query(
            f'(size_in_gb > {gb_threshold}) | (access_datetime < @time_threshold)',
            local_dict={'time_threshold': time_threshold}
        )
        df_append.append(df_levels)

    final_df = pd.concat(df_append).drop_duplicates('levels_pathname')
    return final_df


def to_tree(df):
    root = {'name': 'root', 'children': []}
    for _, row in df.iterrows():
        parts = row['levels_pathname'].strip('/').split('/')
        node = root
        for part in parts:
            match = next((child for child in node['children'] if child['name'] == part), None)
            if not match:
                match = {'name': part, 'children': []}
                node['children'].append(match)
            node = match
        node.update({
            'value': round(row['size_in_gb'], 2),
            'age_in_years': round((datetime.now() - row['access_datetime']).days / 365, 2)
        })
    return root

def prune_empty_children(node):
    if 'children' in node:
        # prune each child first recursively
        new_children = []
        for child in node['children']:
            prune_empty_children(child)
            if 'children' not in child and 'value' not in child:
                # This child is totally empty, don't keep it
                continue
            new_children.append(child)
        if not new_children:
            # no children left -> remove the 'children' key
            del node['children']
        else:
            node['children'] = new_children

# def to_tree(df):
#     root = {'name': 'root', 'children': {}}
#     for _, row in df.iterrows():
#         parts = row['levels_pathname'].strip('/').split('/')
#         node = root
#         for part in parts:
#             if 'children' not in node:
#                 node['children'] = {}
#             if part not in node['children']:
#                 node['children'][part] = {}
#             node = node['children'][part]
#         node.update({
#             'value': round(row['size_in_gb'], 2),
#             'age_in_years': round((datetime.now() - row['access_datetime']).days / 365, 2)
#         })

#     # convert children dicts into lists
#     def dict_to_list(node):
#         if 'children' in node:
#             children_list = []
#             for name, child in node['children'].items():
#                 child_node = {'name': name}
#                 child_node.update(child)
#                 dict_to_list(child_node)
#                 children_list.append(child_node)
#             node['children'] = children_list
#         return node

#     return dict_to_list(root)



@timer_func
def main():
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Analyze project directories")
    parser.add_argument("-f", "--file", help="Input file to analyze")
    parser.add_argument("-o", "--output", help="Output directory")
    args = parser.parse_args()

    with open('config.json') as config_file:
        config = json.load(config_file)
        input_filepath = args.file
        output_dir = args.output

        if not os.path.exists(input_filepath):
            print("The input filepath doesn't exist")
            raise SystemExit(1)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df, max_level = process_list_file(input_filepath)

        final_df = filter_and_aggregate(
            df,
            max_levels=max_level,
            gb_threshold=config.get("gb_threshold", 0.5),
            time_threshold=pd.to_datetime(config.get("access_time_threshold", "2023-01-01"))
        )

        hierarchical_data = to_tree(final_df)
        
        # prune_empty_children(hierarchical_data)

        with open(os.path.join(output_dir, "processed_data.json"), "w") as f:
            json.dump(hierarchical_data, f, indent=4)

if __name__ == "__main__":
    main()

