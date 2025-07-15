import argparse
import os
import tempfile
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContainerClient
from utils import (
    check_date_format,
    schema_check,
    retrieve_csv_data,
    move_file_in_blob,
    write_dataframe_to_parquet,
    AZURE_CONTAINER,
    SCHEMA
)

# Mock data
CONNECTION_STRING = "your_connection_string"
SCHEMA = {
    "column1": "int64",
    "column2": "float64",
    "column3": "object"
}

@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "column1": [1, 2, 3],
        "column2": [1.1, 2.2, 3.3],
        "column3": ["a", "b", "c"]
    })


@pytest.fixture
def mock_blob_service_client():
    return MagicMock(spec=BlobServiceClient)

@pytest.fixture
def mock_container_client():
    return MagicMock(spec=ContainerClient)

def test_check_date_format():
    assert check_date_format("2023-10-01") == "2023-10-01"
    with pytest.raises(argparse.ArgumentTypeError):
        check_date_format("01-10-2023")

def test_schema_check(sample_dataframe):
    assert schema_check(sample_dataframe, SCHEMA) is True

    # Test with mismatched schema
    bad_schema = {
        "column1": "float64",
        "column2": "int64",
        "column3": "object"
    }
    assert schema_check(sample_dataframe, bad_schema) is False

@patch('utils.ContainerClient')
def test_retrieve_csv_data(mock_container_client):
    mock_blob_client = MagicMock()
    mock_container_client.return_value.get_blob_client.return_value = mock_blob_client
    mock_blob_data = MagicMock()
    mock_blob_client.download_blob.return_value = mock_blob_data
    # Utilisez une fonction pour retourner un objet bytes
    def readall_mock():
        return b"column1,column2\n1,1.1\n2,2.2"

    # Configurez le mock des données du blob pour utiliser la fonction
    mock_blob_data.readall = readall_mock

    df = retrieve_csv_data(mock_container_client, "test_blob.csv", delimiter=',')

    assert len(df) == 2
    assert list(df.columns) == ["column1", "column2"]

@patch('utils.BlobServiceClient')
def test_move_file_in_blob(mock_blob_service_client):
    mock_source_blob_client = MagicMock()
    mock_destination_blob_client = MagicMock()
    mock_blob_service_client.get_blob_client.side_effect = [mock_source_blob_client, mock_destination_blob_client]

    move_file_in_blob(mock_blob_service_client, "source_blob", "destination_blob")

    mock_destination_blob_client.start_copy_from_url.assert_called_once_with(mock_source_blob_client.url)

@patch('utils.BlobServiceClient')
def test_write_dataframe_to_parquet(mock_blob_service_client, sample_dataframe):
    mock_container_client = MagicMock()
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_blob_client = MagicMock()
    mock_container_client.get_blob_client.return_value = mock_blob_client

    write_dataframe_to_parquet(mock_blob_service_client, sample_dataframe, "test_df", AZURE_CONTAINER, "test_blob")

    mock_blob_client.upload_blob.assert_called_once()
