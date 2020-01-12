import os
from dask.distributed import LocalCluster, Client, progress
from config import (
    local, gcp_token, project_id, bucket_name, import_data_folder,
    export_data_folder, local_export_folder, bq_db, n_workers,
    threads_per_worker, memory_limit
)
from parameters import (
    uid, import_datasets, target_rename, target, time_index,
    all_columns, all_features_time_index, all_features, categorical_features,
    ts_drop_cols, export_merged_data, export_features_data, export_ts_data
)
from app.ingest.import_data import import_data, unpack_data
from app.ingest.clean_data import (
    clean_sales_data, clean_item_price_data, merge_data
)
from app.feature_engineering.create_agg_features import create_agg_features
from app.utils.utils import downcast_data, export_data
from app.models.time_series import create_all_time_series


if __name__ == '__main__':

    # GCP Configuration
    print('Configuring GCP credentials')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token

    # Dask Configuration
    print('Configuring Dask cluster')
    client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit
    )

    # Processing
    print('Importing & unpacking datasets')
    df_list = list(map(import_data, import_datasets))
    df_sl, df_ip, df_it = unpack_data(df_list)

    print('Cleaning & merging datasets')
    df_sl = clean_sales_data(df_sl, time_index, uid, target_rename)
    df_ip = clean_item_price_data(df_ip, time_index, uid)
    df = merge_data(df_sl, df_ip, df_it, uid, time_index)
    df = downcast_data(df, target)

    print('Exporting merged data')
    export_data(
        df, export_merged_data, local='N', gcs='N', bq='Y'
    )

    print('Creating features')
    df, all_columns, all_features, all_features_time_index = (
        create_agg_features(
            df, time_index, all_columns, all_features,
            all_features_time_index
        )
    )

    print('Exporting features')
    export_data(
        df, export_features_data,  local='N', gcs='N', bq='Y'
    )

    print('Generating Time Series')
    df_ts_all = create_all_time_series(
        df, uid, time_index, target, ts_drop_cols
    )

    print('Exporting Time Series')
    export_data(
        df_ts_all, export_ts_data, local='N', gcs='N', bq='Y'
    )

    client.close()
