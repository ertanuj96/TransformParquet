"""
Parquet Transformation Testing Using Python/C#/Scala
Author: Tanuj Sharma
Start DateTime : 15-10-2023 12:17:15 PM.
Designation : Module Lead Development Consultant
Company : Sequentum India Pvt Ltd
"""

import pyarrow.parquet as pa
# Read data from a Parquet file
table = pa.read_table(r'InputFiles\taobao_list_keywords_pe_legacy.parquet', memory_map=True)
print("RSS: {}MB".format(table.total_allocated_bytes() >> 20))
# Access and manipulate the table's data
schema = table.schema
print("Current File Schema is :-", schema)
columns = table.column_names
print("Current File Columns are :-", columns)