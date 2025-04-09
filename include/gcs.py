"""
Google Cloud Storage utilities.

This module provides functions for interacting with Google Cloud Storage,
specifically for uploading pandas DataFrames as Parquet files.
"""

from airflow.hooks.base import BaseHook
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import tempfile
import pandas as pd


def upload_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str) -> None:
    """Upload a DataFrame to GCS in Parquet format.

    Args:
        df: pandas DataFrame to upload
        bucket_name: GCS bucket name
        blob_name: Full path to the blob in the bucket
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        # Convert DataFrame to PyArrow Table and write to Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(table, temp_file.name)

        # Get GCP connection details
        gcp_conn = BaseHook.get_connection("gcp_standard")

        # Initialize GCS client
        storage_client = storage.Client.from_service_account_info(eval(gcp_conn.extra))

        # Get bucket and create blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Upload the file
        blob.upload_from_filename(temp_file.name)
