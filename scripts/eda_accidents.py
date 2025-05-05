import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load dataset
df = pd.read_csv('data/accidents_2005_2020.csv', encoding='latin1', low_memory=False)

# Basic info
print('Shape:', df.shape)
print('\nColumns:', df.columns.tolist())
print('\nSample rows:')
print(df.head())

# Missing values
print('\nMissing values:')
print(df.isnull().sum())

# Key columns for pipeline and ML
key_columns = ['Num_Acc', 'an', 'dep', 'lat', 'long', 'grav', 'hrmn', 'atm']
print('\nKey columns description:')
print(df[key_columns].describe(include='all'))

# Visualize accidents by hour
df['hour'] = df['hrmn'].str[:2].astype(int, errors='ignore')
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='hour')
plt.title('Accidents by Hour of Day')
plt.xlabel('Hour')
plt.ylabel('Count')
plt.savefig('docs/accidents_by_hour.png')
plt.close()

print('\nEDA complete. Plot saved to docs/accidents_by_hour.png')