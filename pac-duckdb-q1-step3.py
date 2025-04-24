#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

EXPERIMENT = 'pac-duckdb-q1'
INPUT_ZIP = f'./outputs/{EXPERIMENT}-step2.zip'
OUTPUT_DIR = f'./outputs/{EXPERIMENT}-step3'

import os
from typing import List
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import numpy as np
import pickle

import duckdb
import polars as pl
import pyarrow as pa

import zipfile
import numpy as np
import pickle
import json
import io
import parse
import shutil


# In[2]:


# Import saved variables from the first step
INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
templatedf: pl.DataFrame = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/template.pkl', 'rb'))


# In[3]:


# load the json input files from step 2
# {'col': column name as string,
#  'row': group-by column values as dict (effectively a row id),
#  'values': [ 1000x values ] }
alldata = {}

pattern = parse.compile("{n}.json")

with zipfile.ZipFile(INPUT_ZIP, 'r') as zf:
    for filename in zf.namelist():
        if filename.endswith('.json'):
            with zf.open(filename) as f:
                filenumber = int(pattern.parse(filename).named['n'])
                data = json.load(f)
                d: dict = data
                alldata[filenumber] = d
alldata.keys()


# In[4]:


# Cursed data shuffling to reidentify what row goes where, what column goes where, and what the labels should be on everything
colidxes = {}
for col in OUTPUT_COLS:
    colidxes[col] = templatedf.get_column_index(col)
rowidxes = {}
for row in templatedf.select(INDEX_COLS).iter_rows():
    rowidxes[tuple(row)] = rowidxes.get(tuple(row), len(rowidxes))

allinfo = [
    {"colname": adentry['col'],
    "rowid": tuple(adentry['row'].values()),
    "value": adentry['value'][0],
    "colidx": colidxes[adentry['col']],
    "rowidx": rowidxes[tuple(adentry['row'].values())]}
    for adidx, adentry in alldata.items()
]

colnames = {}
rownames = {}
for entry in allinfo:
    colnames[entry['colidx']] = entry['colname']
    rownames[entry['rowidx']] = entry['rowid']


# In[5]:


allgroups = templatedf.select(INDEX_COLS)
allgroups


# Naive reconstruction

# In[6]:


# Naive reconstruction based on the indices of the keys in the templatedf
allcols = INDEX_COLS + OUTPUT_COLS
allrows = allgroups.select(INDEX_COLS).to_numpy().tolist()

allinfo2 = {
    (rowidxes[tuple(adentry['row'].values())], colidxes[adentry['col']]): adentry['value'][0]
    for adidx, adentry in alldata.items()
}

df2 = []
print(allcols)
for row in allrows:
    print(row + [allinfo2.get((rowidxes[tuple(row)], colidxes[col]), None) for col in OUTPUT_COLS])
    df2.append(row + [allinfo2.get((rowidxes[tuple(row)], colidxes[col]), None) for col in OUTPUT_COLS])


# In[7]:


pl.DataFrame(df2, schema=allcols, orient='row').cast(templatedf.schema)


# In[8]:


pl.DataFrame(df2, schema=allcols, orient='row').cast(templatedf.schema).write_csv(os.path.join(OUTPUT_DIR, 'output.csv'))


# Polars-based reconstruction

# In[9]:


# Construct the correct shape of table using only numeric indices for rows and columns
numericdf = pl.DataFrame(allinfo).select(
    pl.col('rowidx'),
    pl.col('colidx'),
    pl.col('value')
).sort(by=['colidx', 'rowidx']).pivot(
    index='rowidx',
    on='colidx',
    values='value',
    maintain_order=True
)
numericdf


# In[10]:


# Add the actual column names
namedcolsdf = numericdf.with_columns(
    pl.col(str(i)).alias(colnames[i])
    for i in [colidxes[c] for c in OUTPUT_COLS]
).drop([str(x) for x in [colidxes[c] for c in OUTPUT_COLS]])
namedcolsdf


# In[11]:


# Use the column names to insert this data into the template table, overwriting the empty columns that exist there
outputdf = templatedf.with_columns(
    pl.col(INDEX_COLS),
).with_columns(
    namedcolsdf.select(pl.all().exclude('rowidx'))
).cast(templatedf.schema)
outputdf


# In[12]:


# zip the OUTPUT_DIR
shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)

