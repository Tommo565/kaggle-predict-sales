from config import test_bucket_name, test_import_data_folder

import_datasets_test = [
    dict(
        local_path='../data/test_data',
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_sales_train.csv',
        df_name='df_sl'
    ),
    dict(
        local_path='../data/test_data',
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_items.csv',
        df_name='df_it'
    ),
    dict(
        local_path='../data/test_data',
        bucket_name=test_bucket_name,
        import_data_folder=test_import_data_folder,
        filename='test_item_categories.csv',
        df_name='df_ic'
    )
]
