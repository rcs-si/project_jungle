import pandas as pd
import dask.dataframe as dd

def analyze_data(df, levels, gb_threshold, time_threshold):
    df_in_use = df
    df_append = pd.DataFrame()

    for l in range(5, levels):
        pjg = df_in_use.groupby(level=[i for i in range(1,l)]).agg({"size_in_gb": "sum", "access_datetime": "min"})

        # filter out those directories/files that are small or not old enough
        filtered_df = pjg[(pjg['size_in_gb'] > gb_threshold) | (pjg['access_datetime'] < time_threshold)]
        filtered_df.to_csv("aggregate_level.csv")
        level_values = filtered_df.index.get_level_values(l)  
        filterfurther_df = df[df.index.get_level_values(l - 1).isin(level_values)]

        # select those rows that have file large or old enough but do not have next index level
        select = filterfurther_df[pd.isna(filterfurther_df.index.get_level_values(l)) & ((filterfurther_df['size_in_gb'] > gb_threshold) | (filterfurther_df['access_datetime'] < time_threshold))]
        df_append = pd.concat([df_append, select])

        # update the df for next round filter
        df = filterfurther_df

    df = df[(df['size_in_gb'] > gb_threshold) | (df['access_datetime'] < time_threshold)]
    final_df = pd.concat([df, df_append])
    return final_df
    

def path_extract(full_pathname, levels, level_limit=False):
    ''' For a path like /gpfs4/project/econdept/x/y/z/A.dat
        extract the levels number of dirs
        so levels=4 --> /gpfs4/project/econdept/x
    '''
    # if level_limit is True, if the number of / characters is less than
    # levels return an empty string.
    if level_limit and full_pathname.count('/') < levels:
        return ''
    return '/'.join(full_pathname.split('/')[0:levels+1])
 

def analyze_data_new(df, max_levels, gb_threshold, time_threshold):
    # Create a column for per-level pathnames.
    df['levels_pathname'] = ''
    df_append = []

    # Is this a good idea? Probably not?
    #full_pathnames = set(df['full_pathname'].unique())
    # Top level of /gpfs4/projectnb/projname is 4.
    for level in range(4, max_levels+1):
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
    final_df = dd.concat(df_append).persist()
    # Filter it for any remaining files that are outside the limits.
    final_df =final_df.query(f'(size_in_gb > {gb_threshold}) | (access_datetime < @t_thresh)',
                                local_dict={'t_thresh':time_threshold})
    final_df = final_df.reset_index()
    return final_df
    


     

if __name__=='__main__':
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    import dask.dataframe as dd

    pdf = pd.DataFrame({
        'full_pathname':['/gpfs4/proj/econ/x/y/z/A.dat', '/gpfs4/proj/econ/x/y/B.dat','/gpfs4/proj/econ/x/C.dat'],
        'size_in_gb':[1,1,1],
        'access_datetime':['2023-06-16 14:05:08.183614016', '2023-06-08 17:45:11.525187015','2023-06-08 17:45:11.518800020']})
    df = dd.from_pandas(pdf)
    df['levels'] = df['full_pathname'].str.count('/')
    levels = 3
    df2 = df[df['levels']> levels]
    df2['levels_pathname'] = df2['full_pathname'].apply(path_extract, args=(levels,),  meta=('levels_pathname', 'str'))
    df3=df2.groupby('levels_pathname').agg({"size_in_gb": "sum", "access_datetime": "min"})
    print(df3.compute())

    