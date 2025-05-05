import pandas as pd

# Input and output files
input_file = "data/accidents_2005_2020_fixed.csv"
output_file = "data/accidents_2005_2020_cleaned.csv"

# Read CSV, handling potential errors
try:
    df = pd.read_csv(input_file, sep=';', encoding='latin1', low_memory=False, quotechar='"')
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit(1)

# Fix header: Remove duplicate Num_Acc and trailing columns
correct_columns = ['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'lum', 'agg', 'int', 'atm', 'col', 'com', 'adr', 'gps', 'lat', 'long', 'dep']
if len(df.columns) > len(correct_columns):
    df.columns = correct_columns + [f"extra_{i}" for i in range(len(df.columns) - len(correct_columns))]
    df = df[correct_columns]  # Drop extra columns

# Remove problematic row (line 958469, 0-based index 958468 after header)
problematic_index = 958468  # Line 958469 - 1 (header)
if problematic_index < len(df):
    print(f"Removing row at index {problematic_index}:")
    print(df.iloc[problematic_index])
    df = df.drop(index=problematic_index).reset_index(drop=True)

# Save cleaned CSV
df.to_csv(output_file, sep=';', encoding='latin1', index=False, quotechar='"')
print(f"Cleaned CSV saved to {output_file}")
print(f"Total rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")