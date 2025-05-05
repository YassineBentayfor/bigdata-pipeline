import pandas as pd
import glob
import os

# Directory with caracteristiques CSVs
data_dir = "data"
output_file = "data/accidents_2005_2020_fixed.csv"

# Find all caracteristiques CSVs
csv_files = glob.glob(os.path.join(data_dir, "caracteristique-*.csv"))

# Read and combine CSVs
dfs = []
for file in sorted(csv_files):
    print(f"Processing {file}...")
    try:
        # Try semicolon separator (common in later years)
        df = pd.read_csv(file, sep=';', encoding='latin1', low_memory=False, quotechar='"')
        dfs.append(df)
    except Exception as e:
        print(f"Error in {file}: {e}")
        # Try comma separator for older files
        df = pd.read_csv(file, sep=',', encoding='latin1', low_memory=False, quotechar='"')
        dfs.append(df)

# Concatenate and save
combined_df = pd.concat(dfs, ignore_index=True)
combined_df.to_csv(output_file, sep=';', encoding='latin1', index=False)
print(f"Combined CSV saved to {output_file}")
print(f"Total rows: {len(combined_df)}")
print(f"Columns: {combined_df.columns.tolist()}")