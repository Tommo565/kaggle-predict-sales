from config import bucket_name, import_data_folder

datasets = [
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
