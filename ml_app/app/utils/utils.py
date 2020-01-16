import pandas as pd
import numpy as np
from dask import delayed
from random import sample
from google.cloud import bigquery


def generate_date_range(df, time_index):
    """
    Summary
    -------
    Generates a complete set of dates based upon the minimum and maximum
    time_index values for the input DataFrame. This is then converted to
    DataFrame format for merging.

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to be exported

    time_index: str
        The name of the column containing the time index

    Returns
    -------
    df_dates: pandas.DataFrame
        A full list of dates in DataFrame format
    Example
    -------
    df_dates = generate_date_range_df(df, time_index)

    """
    df_dates = pd.DataFrame(
        data=pd.date_range(
            start=df[time_index].min(),
            end=df[time_index].max(),
            freq='M'
        ),
        columns=[time_index]
    )

    return df_dates


@delayed
def resample_infill_target(df, time_index, uid, df_dates):
    """
    Summary
    -------
    Resamples a dataframe containing item sales (target) data to monthly and
    generates date rows for missing records via merging an exhaustive list of
    dates generated via the generate_date_range_df function on to the
    DataFrame.

    Delayed and executed in parallel via Dask.

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to resample and infill missing dates for.

    time_index: str
        The name of the column containing the time index

    uid: str

    df_dates: pandas.DataFrame
        DataFrame containing an axhaustive list of date values to generate
        missing date rows

    Returns
    -------
    df_dict: dict
        A list of records transformed from the DataFrame.

    Example
    --------

    """

    # Get the unique uid to infill missing values
    item_id = df[uid].unique().tolist()[0]

    df = (
        df.resample('M')
        .sum()
        .fillna(0)
        .merge(right=df_dates, on=time_index, how='outer')
        .sort_values(by=time_index)
        .assign(item_id=item_id)
        .fillna(0)
        .to_dict(orient='records')
    )

    return df


@delayed
def resample_infill_item_price(df, time_index, uid, df_dates):
    """
    Summary
    -------
    Resamples a dataframe containing item price data to monthly and generates
    date rows for missing records via merging an exhaustive list of dates
    generated via the generate_date_range_df function on to the DataFrame.

    Delayed and executed in parallel via Dask.

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to resample and infill missing dates for.

    time_index: str
        The name of the column containing the time index

    uid: str

    df_dates: pandas.DataFrame
        DataFrame containing an axhaustive list of date values to generate
        missing date rows

    Returns
    -------
    df_dict: dict
        A list of records transformed from the DataFrame.

    Example
    --------

    """

    # Get the unique uid to infill missing values
    item_id = df[uid].unique().tolist()[0]

    df = (
        df.resample('M')
        .mean()
        .fillna(method='ffill')
        .reset_index()
        .merge(right=df_dates, on=time_index, how='outer')
        .assign(item_id=item_id)
        .sort_values(by=time_index)
        .fillna(method='bfill')
        .fillna(method='ffill')
        .to_dict(orient='records')
    )

    return df


def sample_df(df, uid, n):
    """
    Summary
    -------
    Returns a sample dataframe of n unique id's randomly sampled from the input
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


def sample_top_records(df, uid, sort_col, n):
    """
    Summary
    -------
    Returns a sample dataframe of n unique id's randomly sampled from the input
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

    df = df.sort_values(sort_col, ascending=False)

    uid_list = df[uid].unique().tolist()[:n]

    df_sm = df[
        df[uid].isin(uid_list)
    ]

    return df_sm


def downcast_data(df, target):
    """

    """

    # Get float & int cols
    float_cols = [col for col in df if df[col].dtype == "float64"]
    int_cols = [col for col in df if df[col].dtype in ["int64", "int32"]]

    # Downcast float & int cols
    df[float_cols] = df[float_cols].astype(np.float32)
    df[int_cols] = df[int_cols].astype(np.int16)

    return df


def export_data(df, export_data, local='Y', gcs='Y', bq='Y'):
    """
    Summary
    -------
    Exports a dataframe to a GCS bucket, BQ table or local folder depending
    upon the options specified in the parameters.

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
    None

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

        # # Convert the date so BQ doesn't get confused
        # if time_index in df.columns:
        #     df[time_index] = df['date'].dt.strftime('%d-%b-%Y')

        # print(f'    Exporting {filename} to BigQuery')
        # bq_client = bigquery.Client(
        #     project=project_id
        # )

        # table_id = f'{bq_db}.{filename}'
        # job = bq_client.load_table_from_dataframe(
        #     dataframe=df,
        #     destination=table_id
        # )
        # job.result()

        df.to_gbq(
            destination_table=f'{bq_db}.{filename}',
            if_exists='replace'
        )


def import_from_bq(query, project_id):
    """

    """
    df = (
        pd.read_gbq(
            query=query,
            project_id=project_id
        )
    )

    return df
