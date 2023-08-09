import csv

def parse_chunk(lines, outfile):
    for line in lines:
        outfile.write(line + '\n')

def copy_lines(oldfile_path, newfile_path, num_lines=10000):
    with open(oldfile_path, 'r') as infile:
        with open(newfile_path, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                if i >= num_lines:
                    break
                strip_line = line.strip()
                split_line = strip_line.split(maxsplit=11)
                writer.writerow(split_line)
