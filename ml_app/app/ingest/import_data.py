from dask import delayed, compute
import pandas as pd
import gcsfs


def import_data(import_datasets, local='N'):
    """
    Summary
    -------
    Imports a csv file as a dask dataframe. Designed to be applied to a list of
    dicts containing dataset details via map().

    Parameters
    ----------
    import_datasets: dict

    Keys / values for the dataset dict are as follows:

        bucket_name: str
            The name of the GCP bucket containing the csv dataset
        import_data_folder: str
            The folder inside the GCP bucket that contains the
            csv dataset
        filename: str
            The filename of the csv dataset
        df_name: str
            The name to assign the dataframe in the output dict

    local: str
        Specifies whether to import datasets from GCS or from local files.
        If set to 'Y', datasets are imported from data/input_data.
        If not set to 'Y', datasets are imported from GCS buckets.

    Returns
    -------
    output: dict

    Keys / values for the output dict are as follows:

        df_name: str
            The name assigned to the dataframe
        df: pandas.DataFrame
            A dask dataframe containing imported csv data

    Example
    --------
    df_list = list(map(import_data, import_datasets, local='Y')

    """

    local_path = import_datasets['local_path']
    bucket_name = import_datasets['bucket_name']
    import_data_folder = import_datasets['import_data_folder']
    filename = import_datasets['filename']
    df_name = import_datasets['df_name']

    if local == 'Y':

        df = pd.read_csv(
            f'{local_path}/{filename}'
        )

    else:

        df = pd.read_csv(
            f'gcs://{bucket_name}/{import_data_folder}/{filename}'
        )

    df = df.drop_duplicates()
    output = dict(
        df_name=df_name,
        df=df
    )

    return output


def unpack_data(df_list):
    """
    Summary
    -------
    Unpacks the relevent datasets from the input df_list. Returns these as
    individual dataframes. Presently only sales, item price and item category
    data is processed.

    Parameters
    ----------
    df_list: list
        List of dictionaries containing both dataframes and metadata

    Returns
    -------
    df_sl: pandas DataFrame
        DataFrame containing sales data

    df_ip: pandas DataFrame
        DataFrame containing mean item price data

    df_it: pandas DataFrame
        DataFrame containing item category data

    Example
    -------
    df_sl, df_ip, df_it = unpack_data(df_list)


    """

    for df in df_list:
        if df['df_name'] == 'df_sl':

            # Sales data
            df_sl = df['df'][['date', 'item_id', 'item_cnt_day']].copy()

            # Item price data
            df_ip = df['df'][['item_id', 'date', 'item_price']].copy()

        elif df['df_name'] == 'df_it':

            # Item category data
            df_it = df['df'][['item_id', 'item_category_id']].copy()

    return df_sl, df_ip, df_it
