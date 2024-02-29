import csv
import zipfile
import faker
import pandas as pd

# Unzip heart disease dataset
# https://www.kaggle.com/datasets/johnsmith88/heart-disease-dataset
with zipfile.ZipFile('heart.zip', 'r') as z:
    z.extractall()

# import the data to a dataframe
df = pd.read_csv('heart.csv', delimiter=',')

# use faker to add fake names as first column in df
fake = faker.Faker()
faker.Faker.seed(0)
df.insert(0, 'name', [fake.name() for _ in range(len(df))])

# save and overwrite heart.csv
df.to_csv('heart.csv', index=False)
df.to_parquet('heart.parquet', index=False)