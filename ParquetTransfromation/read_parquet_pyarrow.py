"""
Parquet Transformation Testing Using Python/C#/Scala
Author: Tanuj Sharma
Start DateTime : 15-10-2023 12:17:15 PM.
Designation : Module Lead Development Consultant
Company : Sequentum India Pvt Ltd
"""

import pyarrow.parquet as pq
import pyarrow as pa

# Read data from a Parquet file
parquet_file = pq.ParquetFile(r'InputFiles\taobao_list_keywords_pe_legacy.parquet')
parquet_file_schema = parquet_file.schema
file_schema = pq.ParquetSchema()
parquet_file_schema2 = parquet_file.schema_arrow
num_row_groups = parquet_file.num_row_groups

common_metadata = parquet_file.common_metadata

print(parquet_file_schema)
print(parquet_file_schema2)
print(num_row_groups, common_metadata)
# Retrieve the Parquet file metadata, including software information
metadata = parquet_file.metadata

print(metadata)

# Extract the version information of the software that created the file

arrow_table = pq.read_table(r'InputFiles\taobao_list_keywords_pe_legacy.parquet')
# Access and manipulate the table's data
schema = arrow_table.schema
print("Current File Schema is :-", schema)

pandas_df = arrow_table.to_pandas()
pandas_df["RunID"] = 201
print("len of pandas df:", len(pandas_df))
if len(pandas_df) > 0:
    print(pandas_df.head(5))
    print(pandas_df)
    # Specify the Parquet version (1.0) to match the provided metadata
    parquet_version = '1.0'
    table = pa.Table.from_pandas(pandas_df)
    # Write the Table to a Parquet file with the specified version
    output_file = r'OutputFiles\taobao_list_keywords_pe_legacy_201.parquet'
    pq.write_table(table, output_file, version=parquet_version)
    print("Parquet File is Created !")
