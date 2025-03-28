import argparse
import csv
import json
import os
import pandas as pd
import itertools as it

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

def process_list_files(input_filepath, output_filepath):
    max_level = 0
    index_error_raise_count = 0
    other_error = 0
    with open(input_filepath, 'r') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                try:
                    strip_line = line.strip()
                    split_line = strip_line.split(maxsplit=11)
                    pathname = split_line[-1]
                    pathname = pathname.replace('/gpfs4','')
                    levels = count_levels(pathname)
                    max_level = max(max_level, levels)
                    owner = split_line[4]
                    size_in_bytes = split_line[6]
                    full_pathname = split_line[11]
                    full_pathname = full_pathname.replace('/gpfs4','')
                    writer.writerow([owner, size_in_bytes, full_pathname])
                except UnicodeDecodeError:
                    index_error_raise_count += 1
                except Exception as e:
                    other_error += 1
    return max_level, index_error_raise_count, other_error



def load_data(file_path, max_level, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, header=None)
    df.columns = ['owner', 'size_in_bytes', 'full_pathname']
    df['size_in_gb'] = df['size_in_bytes'] / 1e9
    
    split_path = df['full_pathname'].str.split('/', expand=True).iloc[:, 1:]
    df = pd.concat([split_path, df], axis=1)
    index_df = df.set_index(df.columns[:max_level].tolist())
    return index_df
 
    
def conv_leaf_dict(x):
    '''x is the lowest level dictionary containing files:
        node['rprojectnb']['ABC']['QWE']={'x.hap': {'size_in_gb': 0.020849856}, 'y.haps': {'size_in_gb': 0.020937379}}                                          
      Returns the new dict format:
          [{'name': 'x.hap', 'value': 0.020849856}, {'name': 'y.haps', 'value': 0.020937379}]
    '''
    #import pdb;pdb.set_trace()    
    y = []
    for key in x:
        tmp = {'name':key[-1], 'value':x[key]['size_in_gb']}
        y.append(tmp)
    return y

def df_to_hierarchical(df, levels):
    def build_tree(group):
        """Recursively builds the hierarchical dictionary structure."""
        tree = {}
        for key, sub_group in group.groupby(level=0, group_keys=False):
            if sub_group.index.nlevels > 1:
                tree[key] = build_tree(sub_group.droplevel(0))
            else:
                # Convert leaf nodes to the required format
                tree[key] = conv_leaf_dict(sub_group.to_dict(orient="index"))
        return tree

    hierarchical_data = {'name': "root", 'children': []}
    grouped = df.groupby(level=list(range(levels)))

    for keys, group in grouped:
        keys = tuple(dict.fromkeys(keys))  # remove duplicates
        node = hierarchical_data

        # Traverse through the tree to insert the current file
        for key in keys:
            existing_child = next((child for child in node["children"] if child["name"] == key), None)
            if existing_child is None:
                new_child = {"name": key, "children": []}
                node["children"].append(new_child)
                node = new_child
            else:
                node = existing_child

        # Add file data at the final node
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
        file = args.file.split("/")[-1]
        filename = file.split(".")[0]
        output_dir = args.output
        pp_dir = os.path.join(output_dir, "pp")
        if not os.path.exists(pp_dir):
            os.makedirs(pp_dir)
        if not os.path.exists(input_filepath):
            print("The input filepath doesn't exist")
            raise SystemExit(1)

        max_level, index_error_raise_count, other_error = process_list_files(input_filepath, os.path.join(pp_dir, file))
        index_df = load_data(os.path.join(pp_dir, file), max_level)

        final_df = index_df.groupby(level=list(range(max_level))).sum()
        hierarchical_data = df_to_hierarchical(final_df, max_level)

        with open(os.path.join(output_dir, "processed_data.json"), "w") as f:
            json.dump(hierarchical_data, f, indent=4)

if __name__ == "__main__":
    main()
