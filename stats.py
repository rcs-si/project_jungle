# import csv

# def process_list_files(input_filepath, output_filepath):
#     max_level = 0
#     index_error_raise_count = 0
#     other_error = 0
    
#     with open(input_filepath, 'r') as infile:
#         with open(output_filepath, 'w') as outfile:
#             writer = csv.writer(outfile)
#             for i, line in enumerate(infile):
#                 try:
#                     strip_line = line.strip()
#                     split_line = strip_line.split(maxsplit=11)
#                     pathname = split_line[-1]
#                     levels = count_levels(pathname)
#                     max_level = max(max_level, levels)
#                     owner = split_line[4]
#                     size_in_bytes = split_line[6]
#                     size_in_kb = split_line[7]
#                     access_time = split_line[8]
#                     full_pathname = split_line[11]
#                     writer.writerow([owner, size_in_bytes, size_in_kb, access_time, full_pathname])
#                 except UnicodeDecodeError:
#                     index_error_raise_count += 1
#                 except Exception as e:
#                     other_error += 1
#     return max_level, index_error_raise_count, other_error

# def stats_list(output_filepath):
#     bins = [0, 2.5, 5, 7.5, 10, float('inf')]
#     labels = [
#         'less than 2.5',
#         'between 2.5 and 5',
#         'between 5 and 7.5',
#         'between 7.5 and 10',
#         '10 years or more'
#     ]

#     file_counts = {label: 0 for label in labels}
#     size_sums = {label: 0 for label in labels}
#     other_error = 0

#     with open(output_filepath, 'r') as infile:
#         reader = csv.reader(infile)
#         for row in reader:
#             try:
#                 access_time = float(row[2])
#                 size_in_bytes = float(row[1])
#                 size_in_gb = size_in_bytes / 1e9  # Convert bytes to gigabytes

#                 # Calculate years since access time
#                 current_time = pd.Timestamp.now().timestamp()
#                 years_since_access = (current_time - access_time) / (365 * 24 * 60 * 60)

#                 # Assign to bin
#                 for j in range(len(bins) - 1):
#                     if bins[j] <= years_since_access < bins[j + 1]:
#                         file_counts[labels[j]] += 1
#                         size_sums[labels[j]] += size_in_gb
#                         break
#             except Exception as e:
#                 other_error += 1

#     # Generate report
#     report = "\nData Files Report:\n"
#     report += f"Units: Gigabytes (GB)\n"
#     for label in labels:
#         report += f"Number of data files {label} years: {file_counts[label]:,}\n"  # Format with commas
#         report += f"Total size of data {label} years: {size_sums[label]:,.2f} GB\n"  # Format with commas and 2 decimal places
#     report += f"Other errors raised: {other_error:,}\n"  # Format with commas

#     print(report)

# # Test process_list_files and stats_list functions on a sample data file
# process_list_files("data/projectnb_econdept_subset.list", "output/output_processed.csv")
# stats_list("ouput/output_processed.csv")



# import csv
# import pandas as pd

# def process_list_files(input_filepath, output_filepath):
#     max_level = 0
#     index_error_raise_count = 0
#     other_error = 0
    
#     with open(input_filepath, 'r') as infile:
#         with open(output_filepath, 'w') as outfile:
#             writer = csv.writer(outfile)
#             for i, line in enumerate(infile):
#                 try:
#                     strip_line = line.strip()
#                     split_line = strip_line.split(maxsplit=11)
#                     pathname = split_line[-1]
#                     levels = count_levels(pathname)
#                     max_level = max(max_level, levels)
#                     owner = split_line[4]
#                     size_in_bytes = split_line[6]
#                     access_time = split_line[8]
#                     full_pathname = split_line[11]
#                     writer.writerow([owner, size_in_bytes, access_time, full_pathname])
#                 except UnicodeDecodeError:
#                     index_error_raise_count += 1
#                 except Exception as e:
#                     other_error += 1
#     return max_level, index_error_raise_count, other_error

# def load_data(file_path, max_level, delimiter=','):
#     df = pd.read_csv(file_path, delimiter=delimiter, header=None)
#     df.columns = ['owner', 'size_in_bytes', 'access_time', 'full_pathname']
#     df['size_in_gb'] = df['size_in_bytes'].astype(float) / 1e9

#     # transfer access time to human readable format
#     df['access_datetime'] = pd.to_datetime(df['access_time'], unit='s', origin='unix')
#     df = df[['owner', 'size_in_gb', 'access_datetime', 'full_pathname']]
    
#     # create levels of directories and files
#     split_path = df['full_pathname'].str.split('/', expand=True).iloc[:, 1:]
#     df = pd.concat([split_path, df], axis=1)

#     index_df = df.set_index(df.columns[:max_level].tolist())
#     return index_df

# def stats_list(output_filepath, max_level):
#     bins = [0, 2.5, 5, 7.5, 10, float('inf')]
#     labels = [
#         'less than 2.5',
#         'between 2.5 and 5',
#         'between 5 and 7.5',
#         'between 7.5 and 10',
#         '10 years or more'
#     ]

#     file_counts = {label: 0 for label in labels}
#     size_sums = {label: 0 for label in labels}

#     df = load_data(output_filepath, max_level)
#     current_time = pd.Timestamp.now()
#     df['years_since_access'] = (current_time - df['access_datetime']).dt.total_seconds() / (365 * 24 * 60 * 60)

#     for _, row in df.iterrows():
#         years_since_access = row['years_since_access']
#         size_in_gb = row['size_in_gb']

#         # Assign to bin
#         for j in range(len(bins) - 1):
#             if bins[j] <= years_since_access < bins[j + 1]:
#                 file_counts[labels[j]] += 1
#                 size_sums[labels[j]] += size_in_gb
#                 break

#     # Generate report
#     report = "\nData Files Report:\n"
#     report += f"Units: Gigabytes (GB)\n"
#     for label in labels:
#         report += f"Number of data files {label} years: {file_counts[label]:,}\n"  # Format with commas
#         report += f"Total size of data {label} years: {size_sums[label]:,.2f} GB\n"  # Format with commas and 2 decimal places

#     print(report)

# # Test process_list_files and stats_list functions on a sample data file
# max_level, _, _ = process_list_files("data/projectnb_econdept_subset.list", "output/output_processed.csv")
# stats_list("output/output_processed.csv", max_level)




import os
import time
import csv
from datetime import datetime

def count_levels(pathname):
    return pathname.count('/')

def generate_report(file_path, output_filepath):
    # Age bins in years
    bins = [
        (0, 2.5),
        (2.5, 5),
        (5, 7.5),
        (10, float('inf'))
    ]
    bin_counts = {bin_range: 0 for bin_range in bins}
    bin_sizes_gb = {bin_range: 0.0 for bin_range in bins}

    current_time = time.time()
    index_error_raise_count = 0
    other_error = 0

    with open(file_path, 'r') as infile:
        with open(output_filepath, 'w') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["Owner", "Size (Bytes)", "Size (GB)", "Access Time", "Full Pathname", "Age Bin (Years)"])
            for line in infile:
                try:
                    strip_line = line.strip()
                    parts = strip_line.split()
                    if len(parts) < 12:
                        continue

                    size = int(parts[6])  # File size in bytes
                    mtime = float(parts[8])  # Modification time
                    pathname = parts[-1]  # Full pathname

                    # Calculate file age in years
                    age_in_years = (current_time - mtime) / (365 * 24 * 60 * 60)
                    size_gb = size / (1024 ** 3)  # Convert size to GB

                    # Determine which bin the file belongs to
                    age_bin = None
                    for bin_range in bins:
                        if bin_range[0] <= age_in_years < bin_range[1]:
                            bin_counts[bin_range] += 1
                            bin_sizes_gb[bin_range] += size_gb
                            age_bin = f"{bin_range[0]} - {bin_range[1]} years"
                            break

                    # Write details to output file
                    owner = parts[4]
                    access_time = parts[8]
                    writer.writerow([owner, size, size_gb, access_time, pathname, age_bin])

                except UnicodeDecodeError:
                    index_error_raise_count += 1
                except Exception as e:
                    other_error += 1

    # Print the report
    print("\nFile Age Report:\n")
    for bin_range in bins:
        count = bin_counts[bin_range]
        size_gb = bin_sizes_gb[bin_range]
        print(f"Files between {bin_range[0]} and {bin_range[1]} years old: {count} files, {size_gb:.2f} GB")
    
    print(f"\nIndex errors: {index_error_raise_count}")
    print(f"Other errors: {other_error}")

if __name__ == "__main__":
    # uncomment lines as needed for test
    file_path = "data/projectnb_econdept_subset.list"
    # file_path = 'data/projectnb_econdept.list'
    output_filepath = "output/file_age_report.csv"
    # output_filepath = "output/file_age_report_full.csv"
    if os.path.exists(file_path):
        generate_report(file_path, output_filepath)
    else:
        print(f"File not found: {file_path}")
