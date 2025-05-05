import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load dataset
try:
    df = pd.read_csv('data/accidents_2005_2020_cleaned.csv', sep=';', encoding='latin1', low_memory=False, quotechar='"')
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit(1)

# Basic info
print('Shape:', df.shape)
print('\nColumns:', df.columns.tolist())
print('\nSample rows:')
print(df.head())

# Missing values
print('\nMissing values:')
print(df.isnull().sum())

# Key columns for pipeline and ML
key_columns = ['Num_Acc', 'an', 'mois', 'jour', 'hrmn', 'grav', 'lat', 'long', 'dep', 'atm']
print('\nKey columns description:')
try:
    print(df[key_columns].describe(include='all'))
except KeyError as e:
    print(f"Key columns error: {e}. Available columns:", df.columns.tolist())

# Visualize accidents by hour
try:
    df['hour'] = df['hrmn'].str[:2].astype(int, errors='ignore')
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x='hour')
    plt.title('Accidents by Hour of Day')
    plt.xlabel('Hour')
    plt.ylabel('Count')
    plt.savefig('docs/accidents_by_hour.png')
    plt.close()
except Exception as e:
    print(f"Error plotting: {e}")

print('\nEDA complete. Plot saved to docs/accidents_by_hour.png')