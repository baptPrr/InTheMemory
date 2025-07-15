import argparse
from datetime import datetime
import io
import pandas as pd
from azure.storage.blob import ContainerClient

def check_date_format(date_string):
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return date_string
    except ValueError:
        raise argparse.ArgumentTypeError("Wrong date format. Expecting YYYY-MM-DD.")


def schema_check(dataframe : pd.DataFrame, expected_schema:dict):
    if set(dataframe.columns) != set(expected_schema.keys()):
        print("Mismatching columns between dataframe and expected schema")
        return False
    
    for col, col_type in expected_schema.items():
        if str(dataframe[col].dtype) != col_type:
            df_col_type = dataframe[col].dtype
            print(f"Column '{col}' is not the right type : {df_col_type} instead of {col_type}")
            return False
        
    return True

def retrieve_csv_data(container: ContainerClient , blob_name, delimiter=';'):
    blob_client = container.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    data = blob_data.readall()
    csv_data = pd.read_csv(io.BytesIO(data), delimiter=delimiter)
    return csv_data
