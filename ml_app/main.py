import os
from dask.distributed import Client, progress
from config import (
    local, gcp_token, project_id, bucket_name, import_data_folder,
    export_data_folder, local_export_folder, bq_db
)
from parameters import (
    uid, import_datasets, target_rename, target, time_index,
    all_columns, all_features_time_index, all_features, categorical_features,
    export_features_data
)
from app.ingest.import_data import import_data, unpack_data
from app.ingest.clean_data import (
    clean_sales_data, clean_item_price_data, merge_data
)
from app.feature_engineering.create_agg_features import create_agg_features
from app.utils.utils import export_data
from app.models.time_series import create_all_time_series


if __name__ == '__main__':

    # Configuration
    print('Configuring GCP credentials')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token

    print('Configuring Dask cluster')
    client = Client(
        n_workers=4,
        threads_per_worker=4,
        dashboard_address=':8785'
    )

    # Processing
    print('Importing & unpacking datasets')
    df_list = list(map(import_data, import_datasets))
    df_sl, df_ip, df_it = unpack_data(df_list)

    print('Cleaning & merging datasets')
    df_sl = clean_sales_data(df_sl, time_index, uid, target_rename)
    df_ip = clean_item_price_data(df_ip, time_index, uid)
    df = merge_data(uid, df_sl, df_ip, df_it)

    print('Creating features')
    df, all_columns, all_features, all_features_time_index = (
        create_agg_features(
            df, time_index, all_columns, all_features, all_features_time_index
        )
    )

    print('Exporting features')
    export_data(df, export_features_data, local='Y', gcs='N', bq='Y')
