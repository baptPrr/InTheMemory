import argparse
from datetime import datetime
import pandas as pd

from azure.storage.blob import BlobServiceClient
from utils import (
    schema_check,
    retrieve_csv_data, 
    check_date_format, 
    move_file_in_blob,
    AZURE_CONTAINER,
    CONNECTION_STRING,
    SCHEMA
)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dates',
        type=check_date_format,
        nargs='*',
        default=[datetime.now().strftime("%Y-%m-%d")],
        help='Date list (format YYYY-MM-DD) to process. Default to execution date.'
    )
    args = parser.parse_args()
    dates = args.dates
    print(f"Ingestion will process files from date(s) : {dates} ")
    
    storage_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = storage_client.get_container_client(AZURE_CONTAINER)

    # Get referential csv
    clients_df = retrieve_csv_data(container, "clients.csv")
    stores_df = retrieve_csv_data(container, "stores.csv")
    products_df = retrieve_csv_data(container, "products.csv")

    # For each referential df, check format and schema. Move file if schema is wrong
    for ref_blob_name, ref_df in [("clients", clients_df),
                                ("stores", stores_df),
                                ("products", products_df)]:
        if not schema_check(ref_df, SCHEMA[ref_blob_name]):
            print(f"{ref_blob_name} file is corrupted. Moving it to 'errors' folder")
            move_file_in_blob(storage_client, 
                              ref_blob_name + ".csv", 
                              "errors/" + ref_blob_name + "_" + datetime.now().strftime("%Y-%m-%d") + ".csv"
                              )
            if ref_blob_name != "clients":
                continue
            else:
                print("""Clients file is needed to proceed to transactions ingestion, but file is corrupted.
                      Ingestion process is terminated here, waiting for clients file to be repaired.""")

    
    # Remove parenthesis from latlng field
    stores_df["latlng_clean"] = stores_df["latlng"].str.replace('(', '').str.replace(')', '')
    # Extract latitude and longitude from latlng_clean field by splitting at ','
    stores_df[["latitude","longitude"]] = stores_df["latlng_clean"].str.split(',', expand=True)
    # Casting latitude and longitude to the right type
    stores_df['latitude'] = stores_df['latitude'].astype(float)
    stores_df['longitude'] = stores_df['longitude'].astype(float)
    # Drop useless columns
    stores_df.drop(columns=["latlng_clean", "latlng"], inplace=True)

    # Get transactions df (current day or specified dates)
    blob_to_retrieve = []
    for blob in container.list_blobs("transactions"):
        if any([d in blob.name for d in dates]) and blob.name.endswith(".csv"):
            blob_to_retrieve.append(blob.name)

    transactions_df = pd.DataFrame(columns = SCHEMA["transactions"].keys())
    
    for blob in blob_to_retrieve:
        blob_df = retrieve_csv_data(container, blob)
        if not schema_check(blob_df, SCHEMA["transactions"]):
            print(f"{blob} file is corrupted. Moving it to 'errors' folder")
            move_file_in_blob(storage_client,blob,"errors/" + blob)
            continue
        transactions_df = pd.concat([transactions_df, blob_df])
    
    # concat date, hour, minute to make a datetime field (or timestamp)
    transactions_df['datetime'] = pd.to_datetime(transactions_df["date"] + ' ' + transactions_df["hour"].astype(str) + ':' + transactions_df["minute"].astype(str), format='%Y-%m-%d %H:%M')
    # Add account_id from clients_df
    transactions_df = transactions_df.merge(clients_df[['account_id','id']], how="left", left_on="client_id",right_on="id")
    transactions_df.drop(columns=["id"])
    print(transactions_df.head())



    

if __name__ == "__main__":
    main()
    
