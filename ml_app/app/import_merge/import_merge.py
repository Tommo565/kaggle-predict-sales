from dask import delayed, compute
import dask.dataframe as dd


@delayed
def import_data(dataset):
    """
    Imports a csv file as a dask dataframe.
    Designed to be mapped to a list of dicts containing dataset details.
    Delayed.
    """

    bucket_name = dataset['bucket_name']
    import_data_folder = dataset['import_data_folder']
    filename = dataset['filename']
    df_name = dataset['df_name']

    df = dd.read_csv(
        f'gcs://{bucket_name}/{import_data_folder}/{filename}'
    )

    return dict(df_name=df_name, df=df)


@delayed
def merge_data(df_list):
    """
    Merges the three datasets in the dd_out object together.
    Delayed.
    """

    for df in df_list:
        if df['df_name'] == 'df_sl':
            df_sl = df['df'].copy()
        elif df['df_name'] == 'df_it':
            df_it = df['df'].copy()
        elif df['df_name'] == 'df_ic':
            df_ic = df['df'].copy()

    df = df_it.merge(
        right=df_ic,
        left_on='item_category_id',
        right_on='item_category_id',
        how='left'
    )

    df = df_sl.merge(
            right=df,
            left_on='item_id',
            right_on='item_id',
            how='left'
        )
    df['date'] = dd.to_datetime(df['date'])

    return df


def import_merge_data(datasets, bucket_name, import_data_folder):
    """
    Wrapper function for the import_data and merge_data functions
    """

    df_list = list(map(import_data, datasets))
    df_out = merge_data(df_list)
    df = compute(df_out)[0].compute()

    return df
