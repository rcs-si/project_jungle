import csv

def copy_lines(old_file_path, new_file_path, num_lines=10000):
    with open(old_file_path, 'r') as infile:
        with open(new_file_path, 'w') as outfile:
            writer = csv.writer(outfile)
            for i, line in enumerate(infile):
                if i >= num_lines:
                    break
                strip_line = line.strip()
                split_line = strip_line.split(maxsplit=11)
                writer.writerow(split_line)

old_file_path = "/projectnb/scv/atime/projectnb_econdept.list"
new_file_path = "/projectnb/rcs-intern/project_jungle/pp_results.list"

if __name__ == "__main__":
    copy_lines(old_file_path, new_file_path)
