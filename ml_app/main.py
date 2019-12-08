import os
from dask.distributed import Client, progress
from config import (
    gcp_token, bucket_name, import_data_folder,
    local_export_folder
)
from parameters import import_data, export_data
from app.ingest.import_merge import import_merge_data


if __name__ == '__main__':

    # Set the GCP authentication credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token

    # Set the local Dask cluster
    print('Configuring local Dask cluster')
    client = Client(n_workers=3, threads_per_worker=4)

    # Import and merge the datasets
    print('Importing & Merging datasets')
    df = import_merge_data(import_data, export_data)
