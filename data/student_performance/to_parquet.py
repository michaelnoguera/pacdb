"""
Generate the parquet files for the student_performance dataset.
Export tables to parquet files saved here for use in experiments.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

tables = ["student-mat", "student-por"]

schema = pa.schema([
    pa.field("school", pa.dictionary(pa.int32(), pa.string())),          # Categorical: "GP" or "MS"
    pa.field("sex", pa.dictionary(pa.int32(), pa.string())),             # Categorical: "F" or "M"
    pa.field("age", pa.int32()),                                         # Numeric: 15 to 22
    pa.field("address", pa.dictionary(pa.int32(), pa.string())),         # Categorical: "U" or "R"
    pa.field("famsize", pa.dictionary(pa.int32(), pa.string())),         # Categorical: "LE3" or "GT3"
    pa.field("Pstatus", pa.dictionary(pa.int32(), pa.string())),         # Categorical: "T" or "A"
    pa.field("Medu", pa.int32()),                                        # Numeric: 0 to 4
    pa.field("Fedu", pa.int32()),                                        # Numeric: 0 to 4
    pa.field("Mjob", pa.dictionary(pa.int32(), pa.string())),            # Categorical: "teacher", "health", "services", "at_home", or "other"
    pa.field("Fjob", pa.dictionary(pa.int32(), pa.string())),            # Categorical: "teacher", "health", "services", "at_home", or "other"
    pa.field("reason", pa.dictionary(pa.int32(), pa.string())),          # Categorical: "home", "reputation", "course", or "other"
    pa.field("guardian", pa.dictionary(pa.int32(), pa.string())),        # Categorical: "mother", "father", or "other"
    pa.field("traveltime", pa.int32()),                                  # Numeric: 1 to 4
    pa.field("studytime", pa.int32()),                                   # Numeric: 1 to 4
    pa.field("failures", pa.int32()),                                    # Numeric: 0 to 4
    pa.field("schoolsup", pa.bool_()),                                   # Binary: yes or no
    pa.field("famsup", pa.bool_()),                                      # Binary: yes or no
    pa.field("paid", pa.bool_()),                                        # Binary: yes or no
    pa.field("activities", pa.bool_()),                                  # Binary: yes or no
    pa.field("nursery", pa.bool_()),                                     # Binary: yes or no
    pa.field("higher", pa.bool_()),                                      # Binary: yes or no
    pa.field("internet", pa.bool_()),                                    # Binary: yes or no
    pa.field("romantic", pa.bool_()),                                    # Binary: yes or no
    pa.field("famrel", pa.int32()),                                      # Numeric: 1 to 5
    pa.field("freetime", pa.int32()),                                    # Numeric: 1 to 5
    pa.field("goout", pa.int32()),                                       # Numeric: 1 to 5
    pa.field("Dalc", pa.int32()),                                        # Numeric: 1 to 5
    pa.field("Walc", pa.int32()),                                        # Numeric: 1 to 5
    pa.field("health", pa.int32()),                                      # Numeric: 1 to 5
    pa.field("absences", pa.int32()),                                    # Numeric: 0 to 93
    pa.field("G1", pa.int32()),                                          # Numeric: 0 to 20
    pa.field("G2", pa.int32()),                                          # Numeric: 0 to 20
    pa.field("G3", pa.int32())                                           # Numeric: 0 to 20, output target
])

for t in tables:
    data = pd.read_csv(t + ".csv", sep=";")

    boolean_columns = [field.name for field in schema if pa.types.is_boolean(field.type)]
    data[boolean_columns] = data[boolean_columns].replace({'yes': True, 'no': False})

    table = pa.Table.from_pandas(data, schema=schema)

    pq.write_table(table, t + ".parquet")