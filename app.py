import argparse
from datetime import datetime
import json
import pandas as pd

from azure.storage.blob import BlobServiceClient
from utils import schema_check, retrieve_csv_data, check_date_format

AZURE_CONTAINER = "76byj86oc9kf"
REF_BLOB = ["clients", "stores","products"]

with open("connection_string.txt") as f:
    connection_string = f.read()

with open("schema.json") as j:
    schema = json.load(j)

def main():
    parser = argparse.ArgumentParser(description='Script qui accepte une liste de dates.')
    parser.add_argument(
        '--dates',
        type=check_date_format,
        nargs='*',
        default=[datetime.now().strftime("%Y-%m-%d")],
        help='Liste de dates au format AAAA-MM-JJ. Par défaut, utilise la date du jour.'
    )
    args = parser.parse_args()
    dates = args.dates
    
    storage_client = BlobServiceClient.from_connection_string(connection_string)
    container = storage_client.get_container_client(AZURE_CONTAINER)

    # Get referential csv
    clients_df = retrieve_csv_data(container, "clients.csv")
    stores_df = retrieve_csv_data(container, "stores.csv")
    products_df = retrieve_csv_data(container, "products.csv")

    for ref_blob_name, ref_df in [("clients", clients_df),
                                ("stores", stores_df),
                                ("products", products_df)]:
        if not schema_check(ref_df, schema[ref_blob_name]):
            # message + déplacer fichier dans erreurs
            continue
        # print(ref_df.head())
    
    stores_df["latlng_clean"] = stores_df["latlng"].str.replace('(', '').str.replace(')', '')
    stores_df[["latitude","longitude"]] = stores_df["latlng_clean"].str.split(',', expand=True)
    stores_df['latitude'] = stores_df['latitude'].astype(float)
    stores_df['longitude'] = stores_df['longitude'].astype(float)
    stores_df.drop(columns=["latlng_clean", "latlng"], inplace=True)
    print(stores_df.head())


    # for blob in container.list_blobs("transactions"):
    #     print(blob.name)

if __name__ == "__main__":
    main()
    
