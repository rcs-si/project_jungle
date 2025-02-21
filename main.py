import argparse
import csv
import json
import os
import sys
import pandas as pd
import plotly.express as px
from analyze import analyze_data
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
    with open(input_filepath, 'r', encoding='UTF-8', errors='backslashreplace') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                try:
                    strip_line = line.strip()
                    split_line = strip_line.split(maxsplit=11)
                    pathname = split_line[-1]
                    levels = count_levels(pathname)
                    max_level = max(max_level, levels)
                    owner = split_line[4]
                    size_in_bytes = split_line[6]
                    size_in_kb = split_line[7]
                    access_time = split_line[8]
                    full_pathname = split_line[11]
                    writer.writerow([owner, size_in_bytes, size_in_kb, access_time, full_pathname])
                except UnicodeDecodeError:
                    index_error_raise_count += 1
                except Exception as e:
                    other_error += 1
    return max_level, index_error_raise_count, other_error

def load_data(file_path, max_level, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, header=None)
    df.columns = ['owner', 'size_in_bytes', 'size_in_kb', 'access_time', 'full_pathname']
    df['size_in_gb'] = df['size_in_bytes'] / 1e9

    # Transfer access time to human-readable format
    df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
    df = df[['owner', 'size_in_gb', 'access_datetime', 'full_pathname']]
    
    # Create levels of directories and files
    split_path = df['full_pathname'].str.split('/', expand=True).iloc[:, 1:]
    df = pd.concat([split_path, df], axis=1)

    index_df = df.set_index(df.columns[:max_level].tolist())
    return index_df

@timer_func
def main():
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Analyze project directories")
    parser.add_argument("-f", "--file", help="Input file to analyze")
    parser.add_argument("-o", "--output", help="Output directory")
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    
    script_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(script_dir,'config.json')) as config_file:
        config = json.load(config_file)

        input_filepath = args.file
        file = args.file.split("/")[-1]
        filename = file.split(".")[0]

        output_dir = args.output
        pp_dir = output_dir + "/pp/"
        analysis_dir = output_dir + "/analysis/"
        vis_dir = output_dir + "/viz/"

        if not os.path.exists(pp_dir):
            os.makedirs(pp_dir)
        
        if not os.path.exists(analysis_dir):
            os.makedirs(analysis_dir)

        if not os.path.exists(vis_dir):
            os.makedirs(vis_dir)

        if not os.path.exists(input_filepath):
            print(input_filepath)
            print("The input filepath doesn't exist")
            raise SystemExit(1)
    
        max_level, index_error_raise_count, other_error = process_list_files(input_filepath, pp_dir + file)
        print("Index error raised: ", index_error_raise_count)
        print("Other error raised: ", other_error)
        print("-------------------- Finished preprocessing --------------------")

        current_datetime = pd.Timestamp.now()
        years_ago = current_datetime - pd.Timedelta(days=365*config["analysis_parameter"]["years"])
        levels = config["analysis_parameter"]["levels"]
        gb_threshold = config["analysis_parameter"]["gb_threshold"]

        index_df = load_data(pp_dir + file, max_level)
        print("-------------------- Finished loading data --------------------")

        final_df = analyze_data(index_df, levels, gb_threshold, years_ago)
        print("-------------------- Finished analyzing data --------------------")
        analysis_filepath = analysis_dir + filename + ".csv"
        final_df.to_csv(analysis_filepath)

        ### Visualizations
        vis_df = final_df.copy()
        vis_df = vis_df.reset_index()
        #vis_df.fillna("NA", inplace=True)
        vis_df = vis_df.fillna("")
        vis_df["year"] = vis_df["access_datetime"].dt.year
        vis_df["size_in_gb"] = vis_df["size_in_gb"].apply(lambda x: x + 1e-9)

        # Retrieve bins and colors from config
        bins = config["color_mapping"]["bins"]
        colors = config["color_mapping"]["colors"]

        # Assign each entry to a bin based on 'size_in_gb'
        vis_df['size_bin'] = pd.cut(vis_df['size_in_gb'],
                                     bins=[0, 2.5, 5, 7.5, 10, float('inf')],
                                     labels=bins, right=False)

        fig = px.treemap(
            vis_df,
            path=vis_df.columns[2:levels],
            values='size_in_gb',
            color='size_bin',
            color_discrete_map=colors
        )

        # Adding a legend dynamically from config
        legend_text = '<b>Legend:</b><br>'
        for bin_label, color_name in colors.items():
            legend_text += f'<span style="color:{color_name}">{bin_label}</span><br>'

        fig.update_layout(
            annotations=[
                dict(
                    x=1.05,
                    y=1.0,
                    text=legend_text,
                    showarrow=False,
                    align='left',
                    xanchor='left',
                    yanchor='top',
                    xref='paper',
                    yref='paper',
                    bordercolor='black',
                    borderwidth=1
                )
            ],
            margin=dict(t=50, l=25, r=200, b=25)
        )

        fig.update_traces(hovertemplate='labels=%{label}<br>size_in_gb=%{value:.1f}<br>parent=%{parent}<br>id=%{id}<br>size_bin=%{color}<extra></extra>')
        fig.write_html(vis_dir + filename + ".html")

if __name__ == "__main__":
    main()
