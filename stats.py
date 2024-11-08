import os
import time
import csv
from datetime import datetime
import argparse 

def count_levels(pathname):
    return pathname.count('/')

def generate_report(infile, outfile):
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
            # Remove /gpfs4 from the pathname
            pathname = pathname.replace('/gpfs4','')
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
    # Done with the files, close them
    infile.close()
    outfile.close()
    
    # Print the report
    print("\nFile Age Report:\n")
    for bin_range in bins:
        count = bin_counts[bin_range]
        size_gb = bin_sizes_gb[bin_range]
        print(f"Files between {bin_range[0]} and {bin_range[1]} years old: {count} files, {size_gb:.2f} GB")
    
    print(f"\nIndex errors: {index_error_raise_count}")
    print(f"Other errors: {other_error}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Project Jungle",
                                     description="Summarize disk usage by age and owner.")
    # argparse will check to see if this file exists and is readable.
    
    parser.add_argument("-i", "--infile", help="Input file to analyze",
                        type=argparse.FileType('r', encoding='UTF-8',
                        errors='backslashreplace'), required=True)
    parser.add_argument("-o", "--outfile", help="Input file to analyze",
                        type=argparse.FileType('w'), required=True)
    
    args = parser.parse_args()
    
    # infile & outfile are automatically opened by the argparse.FileType
    infile = args.infile 
    outfile = args.outfile
    
    generate_report(infile, outfile)
    

