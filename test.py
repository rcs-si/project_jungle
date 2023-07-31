import os
import sys
sys.path.append(os.getcwd())
print(sys.path)
from parsechunk import copy_lines
from df import load_data

copy_lines('/projectnb/scv/atime/projectnb_econdept.list','/projectnb/rcs-intern/alana/objects/parsed.csv')

df = load_data('/projectnb/rcs-intern/alana/objects/parsed.csv')

df.head()

