import pandas as pd
import argparse
import os
from datetime import datetime


def process_list_file(input_filepath):
    try:
        df = pd.read_csv(
            input_filepath,
            usecols=[4, 6, 8, 11],
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
        raise ValueError("DataFrame is empty after filtering. Check input format.")

    # Remove 'root' user and strip /gpfs4 prefix
    df = df[df['owner'] != 'root'].copy()
    df['full_pathname'] = df['full_pathname'].str.replace('/gpfs4', '', regex=False)

    df['size_in_gb'] = df['size_in_bytes'] / 1e9
    df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s')
    return df


def get_top_files_per_user(df, top_n=10):
    df_sorted = df.sort_values(['owner', 'size_in_gb'], ascending=[True, False])
    df_top = df_sorted.groupby('owner').head(top_n)
    return df_top[['owner', 'size_in_gb', 'full_pathname', 'access_datetime']]


def generate_user_summary(df):
    summary = df.groupby('owner').agg(
        total_size_gb=('size_in_gb', 'sum'),
        file_count=('size_in_gb', 'count')
    ).reset_index()
    return summary


def get_biggest_file_per_user(df):
    return df.sort_values(['owner', 'size_in_gb'], ascending=[True, False]) \
             .groupby('owner').first().reset_index()[['owner', 'size_in_gb', 'full_pathname']]


def main():
    parser = argparse.ArgumentParser(description="List top N largest files per user.")
    parser.add_argument("-f", "--file", required=True, help="Input .list file")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument("-n", "--top", type=int, default=10, help="Top N files per user")
    args = parser.parse_args()

    df = process_list_file(args.file)
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    # Top N files per user
    top_files_df = get_top_files_per_user(df, top_n=args.top)
    top_files_df.to_csv(os.path.join(args.output, "top_files_per_user.csv"), index=False)

    # Summary stats per user
    user_summary = generate_user_summary(df)
    user_summary.to_csv(os.path.join(args.output, "user_storage_summary.csv"), index=False)

    # Top 1 biggest file per user
    biggest_file_df = get_biggest_file_per_user(df)
    biggest_file_df.to_csv(os.path.join(args.output, "biggest_file_per_user.csv"), index=False)

    print("Generated: top_files_per_user.csv")
    print("Generated: user_storage_summary.csv")
    print("Generated: biggest_file_per_user.csv")


if __name__ == "__main__":
    main()
