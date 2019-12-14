from config import (
    bucket_name, import_data_folder, export_data_folder, local_export_folder
)

'''Ingest Parameters'''

import_data = [
    dict(
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='sales_train.csv',
        df_name='df_sl'
    ),
    dict(
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='items.csv',
        df_name='df_it'
    ),
    dict(
        bucket_name=bucket_name,
        import_data_folder=import_data_folder,
        filename='item_categories.csv',
        df_name='df_ic'
    )
]

export_ingest_data = dict(
    bucket_name=bucket_name,
    export_data_folder=export_data_folder,
    local_export_folder=local_export_folder,
    filename='merged_data.csv'
)


'''Feature Engineering Parameters'''

# Dict to rename the target
target_rename = dict(item_cnt_day='target')

# The target for the model
target = target_rename['item_cnt_day']

# The time index
time_index = 'date'

# Categorical features
categorical_features = ['item_id', 'item_category_id']

# Numeric featres
numeric_features = []

# All columns for the dataframe
all_columns = categorical_features + numeric_features + [time_index] + [target]

# All feature columns and the time index for the dataframe
all_features_time_index = (
    categorical_features + numeric_features + [time_index]
)

# All feature columns for the dataframe
all_features = categorical_features + numeric_features
