from config import (
    project_id, bucket_name, import_data_folder, export_data_folder,
    local_export_folder, bq_db
)

'''General Parameters'''
uid = 'item_id'
time_index = 'date'
target_rename = dict(item_cnt_day='target')
target = target_rename['item_cnt_day']

'''Ingest Parameters'''
import_datasets = [
    dict(
        local_path='../data/input_data',
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='sales_train.csv',
        df_name='df_sl'
    ),
    dict(
        local_path='../data/input_data',
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='items.csv',
        df_name='df_it'
    ),
    dict(
        local_path='../data/input_data',
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='item_categories.csv',
        df_name='df_ic'
    ),
    dict(
        local_path='../data/input_data',
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='shops.csv',
        df_name='df_sp'
    )
]


'''Feature Engineering Parameters'''

# Categorical features
categorical_features = ['item_id', 'item_category_id']

# Numeric featres
numeric_features = ['item_price']

# All columns for the dataframe
all_columns = categorical_features + numeric_features + [time_index] + [target]

# All feature columns and the time index for the dataframe
all_features_time_index = (
    categorical_features + numeric_features + [time_index]
)

# All feature columns for the dataframe
all_features = categorical_features + numeric_features

# Features export
export_features_data = dict(
    project_id=project_id,
    bucket_name=bucket_name,
    export_data_folder=export_data_folder,
    local_export_folder=local_export_folder,
    filename='features',
    bq_db=bq_db
)

# Time Series Export
export_ts_data = dict(
    project_id=project_id,
    bucket_name=bucket_name,
    export_data_folder=export_data_folder,
    local_export_folder=local_export_folder,
    filename='time_series',
    bq_db=bq_db
)
