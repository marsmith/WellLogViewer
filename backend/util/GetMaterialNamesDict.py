import pandas as pd

df = pd.read_csv("backend/data/matnames.txt", delimiter="\s\s")

out = "{\n"

for index, row in df.iterrows():
    out += '\t"' + str(row['NAME']) + '":' + str(row['ID']) + ", \n"

out += "};"

print(out)
