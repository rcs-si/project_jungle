import pandas as pd

def analyze_data(df, levels, gb_threshold, time_threshold):
    df_in_use = df
    df_append = pd.DataFrame()

    for l in range(5, levels):
        pjg = df_in_use.groupby(level=[i for i in range(1,l)]).agg({"size_in_gb": "sum", "access_datetime": "min"})

        # filter out those directories/files that are small or not old enough
        filtered_df = pjg[(pjg['size_in_gb'] > gb_threshold) | (pjg['access_datetime'] < time_threshold)]
        # filtered_df.to_csv("aggregate_level.csv")
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
