import pyarrow as pa
import pyarrow.parquet as pq

file_schema = pq.ParquetSchema()

# Define the fields with their names and types
schema = pa.schema([
    ('Keyword', pa.string(), False),     # Required binary (String) field
    ('FromKey', pa.string(), False),     # Required binary (String) field
    ('RunDate', pa.timestamp('ns'), True),  # Optional int96 field (nanoseconds)
    ('weight', pa.int64(), True),         # Optional int64 field
    ('searchtype', pa.string(), True),    # Optional binary (String) field
    ('RunID', pa.int32(), False)         # Required int32 field
])

# Create a PyArrow Table using the schema and some data
table = pa.Table.from_pandas(, Schema_schema=schema)

# Write the Table to a Parquet file
pq.write_table(table, 'data.parquet')
# Create a ParquetSchema object
parquet_schema = pq.ParquetSchema(*schema)

print(parquet_schema)
