import pickle
import pandas as pd

with open("final_df.pk","rb") as f:
   df = pickle.load(f)

print(df)
