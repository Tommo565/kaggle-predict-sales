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

# Col
ts_drop_cols = [
    'cap',
    'floor',
    'trend_lower',
    'trend_upper',
    'Christmas Day',
    'Christmas Day_lower',
    'Christmas Day_upper',
    'Defender of the Fatherland Day',
    'Defender of the Fatherland Day_lower',
    'Defender of the Fatherland Day_upper',
    "International Women's Day",
    "International Women's Day_lower",
    "International Women's Day_upper",
    'National Flag Day',
    'National Flag Day_lower',
    'National Flag Day_upper',
    "New Year's Day",
    "New Year's Day_lower",
    "New Year's Day_upper",
    'Orthodox Christmas Day',
    'Orthodox Christmas Day_lower',
    'Orthodox Christmas Day_upper',
    'Russia Day',
    'Russia Day_lower',
    'Russia Day_upper',
    'Spring and Labour Day',
    'Spring and Labour Day_lower',
    'Spring and Labour Day_upper',
    'Unity Day',
    'Unity Day_lower',
    'Unity Day_upper',
    'Victory Day',
    'Victory Day_lower',
    'Victory Day_upper',
    'monthly',
    'monthly_lower',
    'monthly_upper',
    'multiplicative_terms',
    'multiplicative_terms_lower',
    'multiplicative_terms_upper',
    'yearly',
    'yearly_lower',
    'yearly_upper',
    'additive_terms',
    'additive_terms_lower',
    'additive_terms_upper'
]

# Lagging
cols_to_lag = [target, 'yhat', 'yhat_upper', 'yhat_lower']

# Exports

# Merged Data
export_merged_data = dict(
    project_id=project_id,
    bucket_name=bucket_name,
    export_data_folder=export_data_folder,
    local_export_folder=local_export_folder,
    filename='merged',
    bq_db=bq_db
)

# Features export
export_features_data = dict(
    project_id=project_id,
    bucket_name=bucket_name,
    export_data_folder=export_data_folder,
    local_export_folder=local_export_folder,
    filename='agg_features',
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
