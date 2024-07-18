import pandas as pd
from scipy.stats import boxcox
import numpy as np
import os

files_names = os.listdir("/usr/src/app/input_data/Aggregate_t_a/")
for name in files_names:
    if name.endswith(".csv"):
        file_name = name

df = pd.read_csv(f"/usr/src/app/input_data/Aggregate_t_a/{file_name}")

n_righe_usate = 220000

df=df.head(n_righe_usate)

input_col = "count"

# Calcola il lambda ottimale per la trasformazione Box-Cox
target_values, boxcox_lambda = boxcox(df[input_col] + 1)  # Aggiungi 1 per evitare valori zero o negativi

# Applica la trasformazione Box-Cox
df[input_col] = target_values

#Creiamo la colonna row_count_norm:

#get first value and last value of the column
max_val = df[input_col].iloc[0]
min_val = df[input_col].iloc[-1]

df["row_count_norm"] = (df[input_col] - min_val) / (max_val - min_val)

#write the new dataframe to a new csv file
df.to_csv("/usr/src/app/output_data/FinalJoin_transformed.csv", index=False)





