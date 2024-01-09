
import pandas as pd

def extract_csv(path):
    df = pd.read_csv(path)
    for index, row in df.iterrows():
        print(f"Extracting data... {index}")
        context_str = str(row["context"])
        with open(f"./staging/extracted/{index}.txt", "w") as file:
            file.write(context_str)