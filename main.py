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
            dtype={'owner': str, 'size_in_bytes': float, 'access_time': float, 'full_pathname': str},
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

    # Strip /gpfs4 prefix for cleaner output
    df['full_pathname'] = df['full_pathname'].str.replace('/gpfs4', '', regex=False)

    # Remove root-owned files
    df = df[df['owner'] != 'root'].copy()

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
            f'(size_in_gb > {gb_threshold})', #& (access_datetime < @time_threshold)',
            local_dict={'time_threshold': time_threshold}
        )
        df_append.append(df_levels)

    final_df = pd.concat(df_append).drop_duplicates('levels_pathname')
    return final_df


def to_tree(df):
    root = {'name': '', 'children': []}
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

# def to_tree(df):
#     root = {'name': '', 'children': []}
#     for _, row in df.iterrows():
#         parts = row['levels_pathname'].strip('/').split('/')
#         node = root
#         for i, part in enumerate(parts):
#             match = next((child for child in node['children'] if child['name'] == part), None)
#             if not match:
#                 match = {'name': part, 'children': []}
#                 # Set value=0 for every new non-leaf node
#                 if i < len(parts) - 1:
#                     match['value'] = 0
#                 node['children'].append(match)
#             node = match
#         # At the leaf node, overwrite with true size and age
#         node['value'] = round(row['size_in_gb'], 2)
#         node['age_in_years'] = round((datetime.now() - row['access_datetime']).days / 365, 2)
#     return root



# def to_tree(df):
#     root = {'name': '', 'children': []}
#     for _, row in df.iterrows():
#         parts = row['levels_pathname'].strip('/').split('/')
#         node = root
#         for i, part in enumerate(parts):
#             match = next((child for child in node['children'] if child['name'] == part), None)
#             if not match:
#                 match = {'name': part, 'children': []}
#                 # Set "value" to 0 for non-leaf nodes
#                 if i < len(parts) - 1:
#                     match['value'] = 0
#                 node['children'].append(match)
#             node = match
#         # Only set actual size and age for the leaf node
#         node.update({
#             'value': round(row['size_in_gb'], 3),
#             'age_in_years': round((datetime.now() - row['access_datetime']).days / 365, 2)
#         })
#     return root



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
            node['value'] = 0

def flatten_tree_to_csv(node, parent_path='', rows=None):
    if rows is None:
        rows = []

    if node['name'] == 'root':
        current_path = '/'
    else:
        current_path = os.path.join(parent_path, node['name']) if parent_path else node['name']

    size = node.get('value', 0.0)
    age = node.get('age_in_years', None)

    node_type = 'file' if 'children' not in node else 'directory'

    if age is not None:
        if age < 2.5:
            age_bin = "<2.5"
        elif age < 5:
            age_bin = "2.5–5"
        elif age < 7.5:
            age_bin = "5–7.5"
        elif age < 10:
            age_bin = "7.5–10"
        else:
            age_bin = ">10"
    else:
        age_bin = "N/A"

    if current_path != '':  # skip adding the root row itself
        rows.append({
            'Path': '/' + current_path,
            'Size_GB': round(size, 3),
            'Type': node_type,
            'Age_Bin': age_bin
        })

    for child in node.get('children', []):
        flatten_tree_to_csv(child, current_path, rows)

    return rows

def fill_html_template(template, output_dir, hierarchical_data):
    """Fills the placeholder in an HTML template with hierarchical JSON data."""
    with open(template, 'r') as f:
        html_text = f.read()
    
    filled_html = html_text.replace("~!~!~!~;", json.dumps(hierarchical_data))
    
    output_path = os.path.join(output_dir, 'dash_complete.html')
    with open(output_path, 'w') as f:
        f.write(filled_html)



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
        
        prune_empty_children(hierarchical_data)

        # Flatten to CSV
        rows = flatten_tree_to_csv(hierarchical_data)
        csv_output_path = os.path.join(output_dir, "flattened.csv")
        pd.DataFrame(rows).to_csv(csv_output_path, index=False)


        with open(os.path.join(output_dir, "processed_data.json"), "w") as f:
            json.dump(hierarchical_data, f, indent=4)

        html_template_path = 'test_output/dash.html'
        fill_html_template(html_template_path, output_dir, hierarchical_data)


if __name__ == "__main__":
    main()

