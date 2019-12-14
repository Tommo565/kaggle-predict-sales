import os
from dask.distributed import Client, progress
from config import (
    gcp_token, bucket_name, import_data_folder,
    local_export_folder
)
from parameters import (
    import_data, export_ingest_data, target_rename, target, time_index,
    all_columns, all_features_time_index, all_features, categorical_features
)
from app.ingest.import_merge import (
    import_gcs_data, merge_data, export_data_gcs
)
from app.feature_engineering.clean_data import resample_reindex_data
from app.feature_engineering.create_time_series import process_all_time_series

if __name__ == '__main__':

    # Set the GCP authentication credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token

    # Set the local Dask cluster
    print('Configuring local Dask cluster')
    client = Client(n_workers=4, threads_per_worker=4)

    # Import and merge the datasets
    print('Importing & Merging datasets')
    df_list = list(map(import_gcs_data, import_data))
    df = merge_data(df_list)

    # Transformation & Feature Engineering
    df = resample_reindex_data(
        df, target_rename, target, time_index, all_columns,
        all_features_time_index, all_features, categorical_features
    )
    df = process_all_time_series(df, time_index, target, all_features)
