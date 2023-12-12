import pandas as pd
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler

df = pd.read_csv('/projectnb/rcs-intern/project_jungle/a_results.csv')
scaler = MinMaxScaler()
df['normalized_mod_time'] = scaler.fit_transform(df[['modification_time']].astype(float))

fig = px.scatter(df, x='path_part_4', y='bytes_in_gb', color='normalized_mod_time', color_continuous_scale=px.colors.sequential.Viridis)
fig.show()
fig.write_html('/projectnb/rcs-intern/project_jungle/scatter_plot.html')
