import argparse
import json
import os
import pandas as pd
from timeit import default_timer as timer


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


def process_list_file(input_filepath):
    # import pdb; pdb.set_trace()
    try:
        df = pd.read_csv(
            input_filepath,
            usecols=[4, 6, 8, 11],  # 4: owner, 6: size (bytes), 8: access time, 11: path
            names=['owner', 'size_in_bytes', 'access_time', 'full_pathname'],
            dtype={'owner': str, 'size': float, 'access_time': float, 'full_pathname': str},
            sep='\\s+',
            on_bad_lines='skip',
         #   engine='python',
            encoding_errors='backslashreplace'
        )
    except Exception as e:
        print(f"Failed to read input file: {e}")
        raise

    df = df.dropna()

    if df.empty:
        raise ValueError("DataFrame is empty after filtering. Check the input file format.")

    df['full_pathname'] = df['full_pathname'].str.replace('/gpfs4', '', regex=False)
    max_level = df['full_pathname'].apply(count_levels).max()

    return df, max_level


def conv_leaf_dict(x):
    '''Convert leaf-level dict into a D3.js-compatible format'''
    y = []
    for key in x:
        tmp = {'name': key[-1], 'value': x[key]['size_in_gb']}
        y.append(tmp)
    return y


def df_to_hierarchical(df, levels):
    def build_tree(group):
        tree = {}
        for key, sub_group in group.groupby(level=0, group_keys=False):
            if sub_group.index.nlevels > 1:
                tree[key] = build_tree(sub_group.droplevel(0))
            else:
                tree[key] = conv_leaf_dict(sub_group.to_dict(orient="index"))
        return tree

    hierarchical_data = {'name': "root", 'children': []}
    grouped = df.groupby(level=list(range(levels)))

    for keys, group in grouped:
        keys = tuple(dict.fromkeys(keys))  # remove duplicates
        node = hierarchical_data

        for key in keys:
            existing_child = next((child for child in node["children"] if child["name"] == key), None)
            if existing_child is None:
                new_child = {"name": key, "children": []}
                node["children"].append(new_child)
                node = new_child
            else:
                node = existing_child

        if "children" not in node:
            node["children"] = []
        node["children"].extend(conv_leaf_dict(group.to_dict(orient="index")))

    return hierarchical_data


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

        # Process DataFrame directly in memory
        # df.columns = ['owner', 'size_in_bytes', ''full_pathname']
        # df['size_in_bytes'] = pd.to_numeric(df['size_in_bytes'], errors='raise')
        
        df['size_in_gb'] = df['size_in_bytes'] / 1e9
        # df = df.drop('size_in_bytes', axis = 1)

        split_path = df['full_pathname'].str.split('/', expand=True).iloc[:, 1:]
        df = pd.concat([split_path, df], axis=1)
        index_df = df.set_index(df.columns[:max_level].tolist())

        final_df = index_df.groupby(level=list(range(max_level))).sum()
        hierarchical_data = df_to_hierarchical(final_df, max_level)

        with open(os.path.join(output_dir, "processed_data.json"), "w") as f:
            json.dump(hierarchical_data, f, indent=4)


if __name__ == "__main__":
    main()
