# Project_jungle

Project Disk Usage. This utility generates a report on the last access of the
files in the project space. The utility processes `.list` files that contain
information on the sizes of files/directories and access times. The utility
analyzes these files and filters out files/directories that are either larger
than a gigabyte threshold (defaul is 10 GB) or older that a time threshold
(default is 5 years). The analysis is written to a CSV file and also produces
.html files of interactive visualizations to display the analysis.

## Repository structure

There are 4 main files needed to execute this code:

1. `main.py` - This executes the report generating utility
1. `analyze.py` - Contains routines used to analyze the data
1. `config.json` - Contains parameters such as the years threshold, gigabyte threshold, and number of subdirectory levels to analyze
1. `batch.qsub` - Batch script to execute the utility

## Code execution

The batch script `batch.qsub`executes this utility. It is a task array job.
Each `.list` file is processed as a single task. 

Users must modify the batch script to specify the number of tasks to equal the 
number of `.list` files in the directory

## Input data

Sample data can be found in `/projectnb/scv/atime/`

## Ouput data

Output data is written to a user specifed output directory. After running the
code, there are three subdirectories:

1. `analysis` - contains the `.csv` analysis files
1. `pp` - contains intermediate `.list` files that are cleaned for analysis
1. `viz` - contains all visualization files.
