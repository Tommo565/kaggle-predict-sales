from dask import delayed, compute
import dask.dataframe as dd
import gcsfs


def import_merge_data(import_data, export_data):
    """
    Summary
    -------
    Wrapper function for the import_data and merge_data functions. Imports csv
    datasets from a GCS bucket and merges these together, outputing a Dask
    dataframe.

    Parameters
    ----------
    import_data: list

        A list of dict objects. Keys / values for the dict objects are as
        follows:

            bucket_name: str
                The name of the GCS bucket containing the csv datasets
            import_data_folder: str
                The folder inside the GCS bucket that contains the csv datasets
            filename: str
                The filename of the csv dataset.
            df_name: str
                The name to assign the dataframe in the output dict

    export_data: dict
        Export parameters for the dataframe. Key / values as follows:

            bucket_name: str
                The name of the GCS bucket to export the file to

            export_data_folder: str
                The folder inside the GCP bucket that will hold the file

            filename: str
                The filename for the csv

    Returns
    -------
    df: dask.dataframe
    A dask dataframe containing imported & merged csv data

    Example
    -------
    df = import_merge_data(import_data, export_data)
    """

    df_list = list(map(import_gcs_data, import_data))
    df_out = merge_data(df_list)
    df = compute(df_out)[0].compute()
    df = export_data_gcs(df, export_data)

    return df


@delayed
def import_gcs_data(import_data):
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

    bucket_name = import_data['bucket_name']
    import_data_folder = import_data['import_data_folder']
    filename = import_data['filename']
    df_name = import_data['df_name']

    df = dd.read_csv(
        f'gcs://{bucket_name}/{import_data_folder}/{filename}'
    )
    df.drop_duplicates(inplace=True)

    output = dict(df_name=df_name, df=df)

    return output


@delayed
def resample_time_series(df, frequency):
    """

    """

    df = df[[]]


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
        A list of dict objects. Keys / values for the dict objects are as
        follows:

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


def export_data_gcs(df, export_data):
    """
    Summary
    -------
    Exports a dataframe to a GCS bucket specified in the parameters as a .csv
    file.

    Parameters
    ----------
    df: pandas.DataFrame
        The dataframe to be exported

    export_data: dict
        Export parameters for the dataframe. Key / values as follows:

            bucket_name: str
                The name of the GCS bucket to export the file to

            export_data_folder: str
                The folder inside the GCP bucket that will hold the file

            filename: str
                The filename for the csv

    Returns
    -------
    df: pandas.DataFrame
        The dataframe to be exported. Note that no transformation to this
        occurs within the function.

    Example
    -------
    df = export_data(df, export_data)

    """

    bucket_name = export_data['bucket_name']
    export_data_folder = export_data['export_data_folder']
    local_export_folder = export_data['local_export_folder']
    filename = export_data['filename']

    # GCS export
    df.to_csv(
        path_or_buf=f'gcs://{bucket_name}/{export_data_folder}/{filename}',
        index=False
    )

    # Local export
    df.to_csv(
        path_or_buf=f'{local_export_folder}/{filename}',
        index=False
    )

    return df
