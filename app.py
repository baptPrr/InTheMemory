import argparse
from datetime import datetime
import pandas as pd

from azure.storage.blob import BlobServiceClient
from utils import (
    schema_check,
    retrieve_csv_data, 
    check_date_format, 
    move_file_in_blob,
    write_dataframe_to_parquet,
    AZURE_CONTAINER,
    CONNECTION_STRING,
    SCHEMA
)



def main():
    # Add a parser to detect "dates" parameter
    parser = argparse.ArgumentParser()
    current_date = datetime.now().strftime("%Y-%m-%d")
    parser.add_argument(
        '--dates',
        type=check_date_format,
        nargs='*',
        default=[current_date],
        help='Date list (format YYYY-MM-DD) to process. Default to execution date.'
    )
    args = parser.parse_args()
    dates = args.dates
    print(f"Ingestion will process files from date(s) : {dates} ")

    # Instantiate storage client and container object
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
                              "errors/" + ref_blob_name + "_" + current_date + ".csv"
                              )
            # As clients are necessary to enrich transactions, if clients file is corrupted, stop the execution
            if ref_blob_name != "clients":
                continue
            else:
                print("""Clients file is needed to proceed to transactions ingestion, but file is corrupted.
                      Ingestion process is terminated here, waiting for clients file to be repaired.""")
                return 

    
    # Remove parenthesis from latlng field in stores
    stores_df["latlng_clean"] = stores_df["latlng"].str.replace('(', '').str.replace(')', '')
    # Extract latitude and longitude from latlng_clean field by splitting at ','
    stores_df[["latitude","longitude"]] = stores_df["latlng_clean"].str.split(',', expand=True)
    # Casting latitude and longitude to the right type
    stores_df['latitude'] = stores_df['latitude'].astype(float)
    stores_df['longitude'] = stores_df['longitude'].astype(float)
    # Drop useless columns
    stores_df.drop(columns=["latlng_clean", "latlng"], inplace=True)

    # Write referential dataframes to azure blob storage as parquet
    write_dataframe_to_parquet(storage_client, clients_df,"clients", AZURE_CONTAINER, f"formatted/clients/date={current_date}/clients")
    write_dataframe_to_parquet(storage_client, stores_df,"stores", AZURE_CONTAINER, f"formatted/stores/date={current_date}/stores")
    write_dataframe_to_parquet(storage_client, products_df, "products", AZURE_CONTAINER, f"formatted/products/date={current_date}/products")

    # List all transactions files in blob storage matching current day or specified dates
    blob_to_retrieve = []
    for blob in container.list_blobs("transactions"):
        if any([d in blob.name for d in dates]) and blob.name.endswith(".csv"):
            blob_to_retrieve.append(blob.name)

    # Create empty DataFrame to be completed by trnasactions files.
    transactions_df = pd.DataFrame(columns = SCHEMA["transactions"].keys())

    for blob in blob_to_retrieve:
        blob_df = retrieve_csv_data(container, blob)
        if not schema_check(blob_df, SCHEMA["transactions"]):
            print(f"{blob} file is corrupted. Moving it to 'errors' folder")
            move_file_in_blob(storage_client,blob,"errors/" + blob)
            continue
        transactions_df = pd.concat([transactions_df, blob_df])

    if transactions_df.empty:
        print("No transactions for date {}. Skipping enrichment and writing.")
        return 
    
    # concat date, hour, minute to make a datetime field 
    transactions_df['datetime'] = pd.to_datetime(transactions_df["date"] + ' ' + transactions_df["hour"].astype(str) + ':' + transactions_df["minute"].astype(str), format='%Y-%m-%d %H:%M')
    # Add account_id from clients_df
    transactions_df = transactions_df.merge(clients_df[['account_id','id']], how="left", left_on="client_id",right_on="id")
    transactions_df.drop(columns=["id"], inplace=True)

    # Group transactions per date to write in different partitions in azure blob storage
    daily_dfs = {date: group for date, group in transactions_df.groupby('date')}
    for dt, df in daily_dfs.items():
        write_dataframe_to_parquet(storage_client, df, f'transactions_{dt}', AZURE_CONTAINER, f"formatted/transactions/date={dt}/transactions")


if __name__ == "__main__":
    main()
    
