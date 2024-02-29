import csv
import zipfile
import faker
import pandas as pd

# Unzip lung cancer dataset
# https://www.kaggle.com/datasets/thedevastator/cancer-patients-and-air-pollution-a-new-link/data
with zipfile.ZipFile('lung.zip', 'r') as z:
    z.extractall()

# rename "cancer patient data sets.csv" to "lung.csv"
import os
os.rename('cancer patient data sets.csv', 'lung.csv')

# import the data to a dataframe
df = pd.read_csv('lung.csv', delimiter=',')

# use faker to add fake names as first column in df
fake = faker.Faker()
faker.Faker.seed(0)
df.insert(0, 'name', [fake.name() for _ in range(len(df))])

# rename column "OccuPational Hazards" to "Occupational Hazards"
df.rename(columns={'OccuPational Hazards': 'Occupational Hazards'}, inplace=True)
df.rename(columns={'chronic Lung Disease': 'Chronic Lung Disease'}, inplace=True)
df.rename(columns={'Level': 'Cancer'}, inplace=True)
print(df.info())

# save and overwrite lung.csv
df.to_csv('lung.csv', index=False)
df.to_parquet('lung.parquet', index=False)