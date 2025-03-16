### March 10, 2025

Discussed creating the "flare2.json" format from the nested dictionary
structure created by the pandas dataframe format.

#### task 1
Another recursive function will be needed for the conversion. Here's some
pseudo-code we discussed:
```python
recursive function:  (list(node.keys()), hierarchical_data, node.file_list)

# variable a is the children array at a directory
if not any(d.get('name', None) == 'XYZ' for d in a):
    # does not exist
    a.append({'name':'XYZ','children':[]})

# Now you're guaranteed the directory XYZ 
# is here so you can insert a subdirectory or
# files into its children. 
for x in a:
  if a[x]['name']=='XYZ':
    #.... recursive call if you're insreting another directory, 
    # pass a[x]  into the function.
    #or append your list of file dictionaries
    break
```

Another approach implemented. will check in on github (have a merge conflict)

anyway, something along those lines. 

#### task 2
Test the "indented tree" visualization. 

Indented tree visual works, but I wonder how it would behave for extremely large data.

#### task 3
Implement the pre-computed directory size in Python and add it to the JSON data structure. 
Then modify the indented tree to read that instead of doing its own sum.

in progress - Does this make sense? I believe there is some stuff happening in the back end for d3 that handles this. Github still broken will fix conflict.

#### task 4
Replace the process_data.json file read in the HTMl with an insert of the data directly into the HTML page.
