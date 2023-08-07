def parse_chunk(lines, outfile):
    for line in lines:
        outfile.write(line + '\n')

def copy_lines(oldfile_path, newfile_path, num_lines=10000):
    with open(oldfile_path, 'r') as infile:
        with open(newfile_path, 'w') as outfile:
            lines = []
            for i, line in enumerate(infile):
                if i >= num_lines:
                    break
                lines.append(line.strip())
                if i % num_lines == num_lines - 1:
                    parse_chunk(lines, outfile)
                    lines = []
            if lines:
                parse_chunk(lines, outfile)
