import os
from dataclasses import dataclass
from datetime import datetime
from pprint import pprint
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

@dataclass
class Product:
    Keyword:str
    FromKey:str
    RunDate:datetime
    weight:int
    searchtype:str
    RunID:int




    # table_1_default_schema = pa.schema([
#         ('Keyword', pa.string(), False),  # Required binary (String) field
#         ('FromKey', pa.string(), False),  # Required binary (String) field
#         ('RunDate', pa.timestamp('ns'), True),  # Optional int96 field (nanoseconds)
#         ('weight', pa.int64(), True),  # Optional int64 field
#         ('searchtype', pa.string(), True),  # Optional binary (String) field
#         ('RunID', pa.int32(), False)  # Required int32 field
#     ])





class TransformParquetBase:

    def __init__(self, parquet_file_path: str):
        self.parquet_file_path = parquet_file_path

    def create_test_parquet(self, parquet_version='1.0', output_file=r'OutputFiles\new_data_v1.0_1.parquet'):
        n_legs = pa.array([2, 4, 5, 100])
        animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
        my_schema = pa.schema([
            pa.field('n_legs', pa.int64(), False),
            pa.field('animals', pa.string(), True)],
            metadata={"animals": "Name of the animal species"})
        table = pa.Table.from_arrays([n_legs, animals],

                                     schema=my_schema)
        pq.write_table(table, output_file, version=parquet_version)
        print("Parquet File is Created !")

    def read_file_by_pandas(self):
        df = pd.read_parquet(self.parquet_file_path, engine='pyarrow')
        return df

    def get_file_obj_as_array(self):
        # Read data from a Parquet file
        table = pq.read_table(self.parquet_file_path)

        table_array = [pa.array(range(0, table.num_rows))]
        print("Table Array :", table_array)
        return table_array

    def _from_pandas(self, df):
        """Cast Int64 to object before 'serializing'"""
        for col in df:
            if isinstance(df[col].dtype, pd.Int64Dtype):
                print("Column Name is Int64", col)
                df[col] = df[col].astype('object')
        return pa.Table.from_pandas(df)

    def _to_pandas(self, tbl_):
        """After 'deserializing', recover the correct int type"""
        df = tbl_.to_pandas(integer_object_nulls=True)

        for col in df:
            if (pa.types.is_integer(tbl_.schema.field_by_name(col).type) and
                    pd.api.types.is_object_dtype(df[col].dtype)):
                df[col] = df[col].astype('Int64')

        return df


    def update_runId(self, default_schema, parquet_version='1.0', runIdSortOrder=6):
        pq_df = self.read_file_by_pandas()
        df = self._to_pandas(self._from_pandas(pq_df))
        pq_list = []
        pq_columns = df.columns
        for column in pq_columns:
            for i, r in df.iterrows():
                pq_ = {column: r[column]}
                pq_list.append(pq_)
        # pprint(pq_list)
        table = pq.read_table(self.parquet_file_path, schema=default_schema)
        print(table.schema)
        # table = pa.Table.from_pandas(df, schema=default_schema)

        #table = pa.Table.from_pandas(pq_df,
        #                             schema=default_schema)
        # Assuming you want to update the first column (column 0) with a new value 'new_value'
        new_value = 201

        # Create a new PyArrow Array with the updated values
        new_column_data = [new_value] * len(df)
        updated_table = table.set_column(runIdSortOrder-1, 'RunID', pa.array(new_column_data))
        output_file = os.path.join("OutputFiles", self.parquet_file_path.split("\\").pop())
        pq.write_table(updated_table, output_file, version=parquet_version, store_schema=default_schema)
        read_pq_write_table = pq.read_table(output_file)
        print("Updated Parquet File schema:", read_pq_write_table.schema)
        print("RunID in Parquet File is Updated !")


if __name__ == "__main__":
    input_parquet = r'InputFiles\taobao_list_keywords_pe_legacy.parquet'
    parquetT = TransformParquetBase(input_parquet)
    # parquetT.create_test_parquet()
    table_1_default_schema = pa.schema([
        ('Keyword', pa.string(), False),  # Required binary (String) field
        ('FromKey', pa.string(), False),  # Required binary (String) field
        ('RunDate', pa.timestamp('ns'), True),  # Optional int96 field (nanoseconds)
        ('weight', pa.int64(), True),  # Optional int64 field
        ('searchtype', pa.string(), True),  # Optional binary (String) field
        ('RunID', pa.int32(), False)  # Required int32 field
    ])
    parquetT.update_runId(table_1_default_schema)










