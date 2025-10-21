#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

import json
import os
import pickle

import parse
import polars as pl

from timer import Timer


def run_step_3(
        experiment: str,
        input_dir: str,
        output_dir: str,
        index_cols: list[str],
        output_cols: list[str],
        templatedf_path: str
):
    #EXPERIMENT = "asdf"
    #INPUT_DIR = f"./outputs/{EXPERIMENT}-step2"
    #OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step3"
    #INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
    #OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
    #templatedf_path = f"./outputs/{EXPERIMENT}-step2/templatedf.csv"
    EXPERIMENT = experiment
    INPUT_DIR = input_dir
    OUTPUT_DIR = output_dir
    INDEX_COLS = index_cols
    OUTPUT_COLS = output_cols
    templatedf_path = templatedf_path


    # In[ ]:


    # Timer setup
    timer = Timer(experiment=EXPERIMENT, step="step3", output_dir="./times")

    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


    # In[ ]:


    USING_DEFAULT_INDEX_COLS = False
    if len(INDEX_COLS) == 0:
        print("No index cols provided, using default index cols")
        USING_DEFAULT_INDEX_COLS = True
        INDEX_COLS = ["rank"]


    # In[ ]:


    # Import saved variables from the first step
    timer.start("load_templatedf")
    templatedf: pl.DataFrame = pickle.load(open(templatedf_path, 'rb'))
    timer.end()


    # In[ ]:


    # load the json input files from step 2
    # {'col': column name as string,
    #  'row': group-by column values as dict (effectively a row id),
    #  'values': [ 1000x values ] }
    timer.start("load_all_json_data")
    alldata = {}

    pattern = parse.compile("{n}.json")

    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.json'):
            filenumber = int(pattern.parse(filename).named['n'])
            with open(os.path.join(INPUT_DIR, filename), 'r') as f:
                data = json.load(f)
                d: dict = data
                alldata[filenumber] = d
    alldata.keys()
    timer.end()


    # In[ ]:


    # Make sure the data types of the row fields are correct
    timer.start("determine_cell_locations")
    for i in range(len(alldata)):
        alldata[i]['row'] = pl.DataFrame(alldata[i]['row']).cast(templatedf.select(INDEX_COLS).schema).to_dicts()[0]

    # The response is a list of many NUM_TRIALS trials, we only need one so we just take the first
    from pac_duckdb_step2 import NUM_TRIALS
    for i in range(len(alldata)):
        if isinstance(alldata[i]['value'], list) and len(alldata[i]['value']) == NUM_TRIALS:
            alldata[i]['value'] = alldata[i]['value'][0]
        elif alldata[i]['value'] in [[], [None], None]:
            alldata[i]['value'] = None

    # In[ ]:


    allgroups = templatedf.select(INDEX_COLS)
    allgroups


    # In[ ]:


    # Cursed data shuffling to reidentify what row goes where, what column goes where, and what the labels should be on everything
    colidxes = {}
    for col in OUTPUT_COLS:
        colidxes[col] = templatedf.get_column_index(col)
    rowidxes = {}
    for row in templatedf.select(INDEX_COLS).iter_rows(named=True):
        rowidxes[tuple(row.values())] = allgroups.with_row_index().filter(
            pl.col(k).eq(v)
            for k, v in row.items()
        ).select("index").item()
        print(row)

    allinfo = [
        {"colname": adentry['col'],
        "rowid": tuple(adentry['row'].values()),
        "value": adentry['value'],
        "colidx": colidxes[adentry['col']],
        "rowidx": rowidxes[tuple(adentry['row'].values())]}
        for adidx, adentry in alldata.items()
    ]

    colnames = {}
    rownames = {}
    for entry in allinfo:
        colnames[entry['colidx']] = entry['colname']
        rownames[entry['rowidx']] = entry['rowid']
    timer.end()


    # Naive reconstruction

    # In[ ]:


    # Naive reconstruction based on the indices of the keys in the templatedf
    timer.start("reconstruct_dataframe")
    allcols = INDEX_COLS + OUTPUT_COLS
    allrows = allgroups.select(INDEX_COLS).to_numpy(structured=True).tolist()


    allinfo2 = {
        (rowidxes[tuple(adentry['row'].values())], colidxes[adentry['col']]): adentry['value']
        for adidx, adentry in alldata.items()
    }

    df2 = []
    print(allcols)
    for row in allrows:
        print(row, [allinfo2.get((rowidxes[tuple(row)], colidxes[col]), None) for col in OUTPUT_COLS])
        df2.append([*row, *[allinfo2.get((rowidxes[tuple(row)], colidxes[col]), None) for col in OUTPUT_COLS]])


    # In[ ]:


    df = pl.DataFrame(df2, schema=allcols, orient='row')

    if USING_DEFAULT_INDEX_COLS: # If we used default index cols, remove them from output
        df = df.select(OUTPUT_COLS)

    timer.end()
    df


    # In[ ]:


    timer.start("write_output_json")
    df.write_json(os.path.join(OUTPUT_DIR, 'output.json'))
    df.write_csv(os.path.join(OUTPUT_DIR, 'output.csv'))
    timer.end()

