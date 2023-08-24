import csv 

def get_line(filepath):
    """
    This function is a generator that produces a split line of text when called
    
    Input: Filepath with filename to be parsed line by line.
    
    Yield: Array of strings.   
    """
    with open(filepath, encoding="ascii", errors="replace") as file:
        for line in file:
            split_line = line.split(maxsplit=11) # split the string into an array with 12 elements
            l1 = split_line[:-1] # l1 is everything but the last element
            l2 = split_line[-1].strip() # l2 is the last element stripped of preceding and trailing white space 
            l1.append(l2) # append l2 to l1
            yield l1

in_filepath = "/projectnb/scv/atime/projectnb_econdept.list"
out_filepath = "projectnb_econdept.csv"

n_lines = 3
gen = get_line(in_filepath)

with open(out_filepath, 'w', newline='', encoding='utf8') as file:
    writer = csv.writer(file)
    for i in range(n_lines):
        prc_line = next(gen) # ask for the next line in the file
        writer.writerow(prc_line) # write the row to a csv file