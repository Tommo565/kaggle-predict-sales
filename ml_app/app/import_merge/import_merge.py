from dask import delayed, compute
import dask.dataframe as dd


@delayed
def import_data(dataset):
    """
    Summary
    -------
    Imports a csv file as a dask dataframe. Designed to be applied to a list of
    dicts containing dataset details via map(). Processing is delayed via the
    Dask @delayed decorator. See the Dask documentation for more details.

    Parameters
    ----------
    dataset: dict

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

    Returns
    -------
    output: dict

    Keys / values for the output dict are as follows:

        df_name: str
            The name assigned to the dataframe
        df: dask.dataframe
            A dask dataframe containing imported csv data

    Example
    --------
    df_list = list(map(import_data, datasets))

    """

    bucket_name = dataset['bucket_name']
    import_data_folder = dataset['import_data_folder']
    filename = dataset['filename']
    df_name = dataset['df_name']

    df = dd.read_csv(
        f'gcs://{bucket_name}/{import_data_folder}/{filename}'
    )

    output = dict(df_name=df_name, df=df)

    return output


@delayed
def merge_data(df_list):
    """
    Summary
    -------
    Merges the three datasets in the df_list object together and formats the
    date column in the df_sl dataset as DateTime. Processing is delayed via the
    Dask @delayed decorator. See the Dask documentation for more details.

    Parameters
    ----------
    df_list: list

    A list of dict objects. Keys / values for the dict objects are as follows:

        df_name: str
            The name assigned to the dataframe
        df: dask.dataframe
            A dask dataframe containing imported csv data

    Returns
    -------
    df: dask.dataframe
    A dask dataframe containing imported & merged csv data

    Example
    -------
    df_out = merge_data(df_list)

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


def import_merge_data(datasets):
    """
    Summary
    -------
    Wrapper function for the import_data and merge_data functions. Imports csv
    datasets from a GCP GCS bucker and merges these together, outputing a Dask
    dataframe.

    Parameters
    ----------
    datasets: list

    A list of dict objects. Keys / values for the dict objects are as follows:

        bucket_name: str
            The name of the GCP bucket containing the csv datasets
        import_data_folder: str
            The folder inside the GCP bucket that contains the
            csv datasets
        filename: str
            The filename of the csv dataset
        df_name: str
            The name to assign the dataframe in the output dict

    Returns
    -------
    df: dask.dataframe
    A dask dataframe containing imported & merged csv data

    Example
    -------
    df = import_merge_data(datasets)
    """

    df_list = list(map(import_data, datasets))
    df_out = merge_data(df_list)
    df = compute(df_out)[0].compute()

    return df
