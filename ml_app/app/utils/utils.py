import pandas as pd
from random import sample


def reindex_by_date(df, dates):
    """
    Summary
    -------
    Generates a complete monthly DateTime index and reindexes a DateTime index
    on a pandas dataframe to add in missing date values. Assigns a 0 value to
    any fields which have are NaN.

    Should be applied to a dataframe via groupby.

    Parameters
    ----------
    df: pandas.DataFrame
        The dataframe to re-index

    dates: pandas.DateTimeIndex
        The dates to re-index with

    Returns
    -------
    df: pandas.DataFrame
        A reindexed dataframe with a full DateTime index

    Example
    --------
    df = (
        df.groupby(['item_id', 'item_category_id'])
        .apply(reindex_by_date, dates=dates)
    )

    """

    df = df.drop('item_id', axis=1).reindex(dates).fillna(0)
    return df


def sample_df(df, uid, n):
    """
    Summary
    -------
    Returns a sample dataframe of n unique id's from withing the input
    dataframe.

    Parameters
    ----------

    df: pandas.DataFrame
        The dataframe to be randomly sampled.

    uid: str
        The unique id columns to select sample records from.

    n: int
        The number of samples to select.

    Returns
    -------
    df: pandas.DataFrame
        A dataframe containing n uid samples.

    Example
    -------
    df = sample_df(df, uid, n)
    """

    uid_list = df[uid].unique().tolist()

    sample_uids = sample(uid_list, n)

    df = df[
        df[uid].isin(sample_uids)
    ]

    return df


def export_data(df, export_data, local='Y', gcs='Y', bq='Y'):
    """
    Summary
    -------
    Exports a dataframe to a GCS bucket and/or local folder specified in the 
    export_data parameter

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

    # Unpack the input dictionary
    project_id = export_data['project_id']
    bucket_name = export_data['bucket_name']
    export_data_folder = export_data['export_data_folder']
    local_export_folder = export_data['local_export_folder']
    filename = export_data['filename']
    bq_db = export_data['bq_db']

    if gcs == 'Y':
        # GCS export
        print(f'    Exporting {filename} to GCS')

        df.to_csv(
            f'gcs://{bucket_name}/{export_data_folder}/{filename}.csv',
            index=False,
        )

    if local == 'Y':
        # Local export
        print(f'    Exporting {filename} locally')

        df.to_csv(
            f'{local_export_folder}/{filename}.csv',
            index=False,
        )

    if bq == 'Y':
        print(f'    Exporting {filename} to BigQuery')
        df.to_gbq(
            project_id=project_id,
            destination_table=f'{bq_db}.{filename}',
            if_exists='replace'
        )

    return df
