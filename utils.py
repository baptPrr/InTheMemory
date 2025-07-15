import argparse
from datetime import datetime
import io
import json
import pandas as pd
from azure.storage.blob import ContainerClient, BlobServiceClient

AZURE_CONTAINER = "76byj86oc9kf"
REF_BLOB = ["clients", "stores","products"]

with open("connection_string.txt") as f:
    CONNECTION_STRING = f.read()

with open("schema.json") as j:
    SCHEMA = json.load(j)

def check_date_format(date_string : str):
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return date_string
    except ValueError:
        raise argparse.ArgumentTypeError("Wrong date format. Expecting YYYY-MM-DD.")


def schema_check(dataframe : pd.DataFrame, expected_schema : dict):
    if set(dataframe.columns) != set(expected_schema.keys()):
        print("Mismatching columns between dataframe and expected schema")
        return False
    
    for col, col_type in expected_schema.items():
        if str(dataframe[col].dtype) != col_type:
            df_col_type = dataframe[col].dtype
            print(f"Column '{col}' is not the right type : {df_col_type} instead of {col_type}")
            return False
        
    return True

def retrieve_csv_data(container: ContainerClient , blob_name : str, delimiter : str =';'):
    blob_client = container.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    data = blob_data.readall()
    csv_data = pd.read_csv(io.BytesIO(data), delimiter=delimiter)
    return csv_data

def move_file_in_blob(storage_client: BlobServiceClient,
                      source_blob_name : str,
                      destination_blob_name : str,
                      source_container_name : str = AZURE_CONTAINER,
                      destination_container_name : str = AZURE_CONTAINER,
                      delete : bool = False):

    # get blob clients
    source_blob_client = storage_client.get_blob_client(container=source_container_name, blob=source_blob_name)
    destination_blob_client = storage_client.get_blob_client(container=destination_container_name, blob=destination_blob_name)

    # copy blob from source to destination
    destination_blob_client.start_copy_from_url(source_blob_client.url)

    # wait for copy to be finished
    while True:
        props = destination_blob_client.get_blob_properties()
        if props.copy.status != 'pending':
            break

    # If asked, delete the source blob
    if delete:
        source_blob_client.delete_blob()

    print(f"File moved from {source_container_name}/{source_blob_name} to {destination_container_name}/{destination_blob_name}")

def write_dataframe_to_parquet(storage_client: BlobServiceClient,
                               df: pd.DataFrame, df_name : str,
                               container_name:str,
                               blob_prefix:str):
    
    # Créer un conteneur client
    container_client = storage_client.get_container_client(container_name)

    # Créer un buffer en mémoire pour le fichier Parquet
    buffer = io.BytesIO()

    # Écrire le DataFrame en Parquet dans le buffer
    df.to_parquet(buffer, engine='pyarrow', compression="snappy")

    # Réinitialiser la position du buffer
    buffer.seek(0)

    # Télécharger le fichier Parquet vers Azure Blob Storage
    blob_client = container_client.get_blob_client(f"{blob_prefix}.parquet")
    blob_client.upload_blob(buffer, overwrite=True)
    print(f"Dataframe {df_name} successfully transformed and uploaded to Blob '{blob_prefix}.parquet'")
