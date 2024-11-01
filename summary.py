import csv


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
