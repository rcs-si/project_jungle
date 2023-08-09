# This script takes an input file path, 
# parses the input file line by line
# processes each line
# writes an output csv file 
import csv

in_filepath = "/projectnb/scv/atime/projectnb_econdept.list"
out_filepath = "projectnb_econdept.csv"

with open(out_filepath, 'w', newline='', encoding='utf8') as file:
    writer = csv.writer(file)
    for line in open(in_filepath, encoding="ascii", errors="replace"):
        split_line = line.split(maxsplit=11)
        l1 = split_line[:-1]
        l2 = split_line[-1].strip()
        l1.append(l2)
        writer.writerow(l1)
