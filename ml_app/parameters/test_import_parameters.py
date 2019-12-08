from config import test_bucket_name, test_import_data_folder

import_merge_test_data = [
    dict(
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_sales_train.csv',
        df_name='df_sl'
    ),
    dict(
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_items.csv',
        df_name='df_it'
    ),
    dict(
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_item_categories.csv',
        df_name='df_ic'
    )
]

export_test_data = dict(
    bucket_name=test_bucket_name,
    export_data_folder=test_import_data_folder,
    filename='test_merged_data'
)
