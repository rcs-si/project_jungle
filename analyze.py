import pandas as pd

def path_extract(full_pathname, levels, level_limit=False):
    ''' For a path like /project/econdept/x/y/z/A.dat
        extract the levels number of dirs
        so levels=3 --> /project/econdept/x
    '''
    # if level_limit is True, if the number of / characters is less than
    # levels return an empty string.
    if level_limit and full_pathname.count('/') < levels:
        return ''
    return '/'.join(full_pathname.split('/')[0:levels+1])
 

def analyze_data(df, max_levels, gb_threshold, time_threshold):
    ''' Sums up levels below the max_levels. Removes things too small under the gb_threshold.
        Remove things too new per the time_threshold '''
    # Create a column for per-level pathnames.
    df['levels_pathname'] = ''
    df_append = []
    # Top level of /projectnb/projname is 3.
    for level in range(3, max_levels+1):
        # Create or replace a column in a portion of df to hold the paths for this level.
        df_levels = df[df['levels'] >= level]
        df_levels['levels_pathname'] = df_levels['full_pathname'].apply(path_extract, args=(level,),  meta=('levels_pathname', 'str'))
        # Filter df_levels, keep only the rows where this is the deepest we want to go.
        #df_levels = df_levels[df_levels['levels_pathname']==df_levels['full_pathname']]   
        # Aggregate to get the info we want.
        df_levels=df_levels.groupby('levels_pathname').agg({"size_in_gb": "sum", "access_datetime": "min"})
        # filter out those directories/files that are too small or not old enough
        df_levels = df_levels.query(f'(size_in_gb > {gb_threshold}) | (access_datetime < @t_thresh)',
                                    local_dict={'t_thresh':time_threshold})
        #df_levels.to_csv("aggregate_level.csv", single_file=True)
        df_append.append(df_levels)
    # Finally concat the list of dataframes into one dataframe
    final_df = pd.concat(df_append) 
    # Filter it for any remaining files that are outside the limits.
    final_df =final_df.query(f'(size_in_gb > {gb_threshold}) | (access_datetime < @t_thresh)',
                                local_dict={'t_thresh':time_threshold})
    # Now look up the size_in_gb in the original df and store that in 
    # final_df. Match on the levels_pathname column.
    final_df = final_df.reset_index()
    tmp_df = df[['full_pathname','size_in_gb']].rename(columns={'size_in_gb':'dir_size_in_gb'})
    # These are explicitly cast as strings to avoid warnings in the merge. 
    final_df = final_df.astype({"levels_pathname": "string"})
    tmp_df = tmp_df.astype({"full_pathname": "string"})
    final_df = final_df.merge(tmp_df, left_on='levels_pathname',right_on='full_pathname')
    final_df = final_df.reset_index()
    return final_df



     

if __name__=='__main__':
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)

    df = pd.DataFrame({
        'full_pathname':['/gpfs4/proj/econ/x/y/z/A.dat', '/gpfs4/proj/econ/x/y/B.dat','/gpfs4/proj/econ/x/C.dat'],
        'size_in_gb':[1,1,1],
        'access_datetime':['2023-06-16 14:05:08.183614016', '2023-06-08 17:45:11.525187015','2023-06-08 17:45:11.518800020']})

    df['levels'] = df['full_pathname'].str.count('/')
    levels = 3
    df2 = df[df['levels']> levels]
    df2['levels_pathname'] = df2['full_pathname'].apply(path_extract, args=(levels,))
    df3=df2.groupby('levels_pathname').agg({"size_in_gb": "sum", "access_datetime": "min"})
    print(df3)

    
