import pandas as pd

df = pd.read_table("backend/data/well_logs.subf", delimiter="\t", header=21, encoding = "ISO-8859-1", skiprows=[22])
df = df.head(500)
df.to_csv("backend/data/well_logs_short.csv", sep=",")