import pandas as pd
import numpy as np
from dask import compute
from app.utils.utils import (
    generate_date_range, resample_infill_target, resample_infill_item_price
)


def clean_sales_data(df_sl, time_index, uid, target_rename):
    """
    Summary
    -------
    Cleans and transforms the sales dataset through aggregating the target,
    resampling the data to a monthly frequency and infilling missing months.

    Parameters
    ----------
    df_sl: pandas DataFrame
        The sales dataframe to be cleaned

    uid: str
        The name of the column containing the unique id

    time_index: str
        The name of the column containing the time index

    target_rename: dict
        dict containing the data to rename the target variable

    Returns
    -------
    df_sl: pandas DataFrame
        The cleaned and transformed sales data.

    Example
    --------
    df_sl = clean_sales_data(df_sl, time_index, uid, target_rename)
    """

    # Aggregate target to account for missing shop_id
    df_sl = (
        df_sl
        .assign(date=pd.to_datetime(df_sl[time_index]))
        .rename(target_rename, axis=1)
        .groupby([uid, time_index])
        .sum()
        .reset_index()
        .sort_values([uid, time_index])
    )

    # Generate total date range for the df to infill missing rows
    df_dates = generate_date_range(df_sl, time_index)

    # Group the DataFrame to process uid records individually
    df_sl_gp = (
        df_sl.set_index(time_index)
        .groupby(uid)
    )

    # Create an output list to append records to
    df_sl_output = []

    for group in df_sl_gp.groups:
        df_sl_ind = df_sl_gp.get_group(group)
        df_sl_ind = resample_infill_target(
            df_sl_ind, time_index, uid, df_dates
        )
        df_sl_output.append(df_sl_ind)

    # Compute the output
    df_sl_output = compute(df_sl_output)[0]

    # Create a blank list to process the output into
    records_list = []

    # Create a list of records for conversion to a dataframe
    for item in df_sl_output:
        for record in item:
            records_list.append(record)

    df_sl = pd.DataFrame(records_list)

    return df_sl


def clean_item_price_data(df_ip, time_index, uid):
    """
    Summary
    -------

    Parameters
    ----------
    df_ip: pandas DataFrame
        The item price dataframe to be cleaned

    time_index: str
        The name of the column containing the time index

    uid: str
        The name of the column containing the unique id

    Returns
    -------
    df_ip: pandas DataFrame
        The cleaned and transformed item price data.

    Example
    --------
    df_ip = clean_item_price_data(df_ip, time_index, uid)

    """

    # Convert to DateTime
    df_ip[time_index] = pd.to_datetime(df_ip[time_index])

    # Group the DataFrame to process uid records individually
    df_ip_gp = (
        df_ip.set_index(time_index)
        .groupby(uid)
    )

    # Generate total monthly date range for the df
    df_dates = generate_date_range(df_ip, time_index)

    # Create an output list to append records to
    df_ip_output = []

    # Resample to monthly & infill missing date rows
    for group in df_ip_gp.groups:
        df_ip_ind = df_ip_gp.get_group(group)
        df_ip_ind = resample_infill_item_price(
            df_ip_ind, time_index, uid, df_dates
        )
        df_ip_output.append(df_ip_ind)

    # Compute the output
    df_ip_output = compute(df_ip_output)[0]

    # Create a blank list to process the output into
    records_list = []

    # Create a list of records for conversion to a dataframe
    for item in df_ip_output:
        for record in item:
            records_list.append(record)

    df_ip = pd.DataFrame(records_list)

    return df_ip


def merge_data(df_sl, df_ip, df_it, uid, time_index, target):
    """
    Summary
    -------
    Merges the sales, item price and item datasets together

    Parameters
    ----------
    df_sl: pandas DataFrame
        The sales dataframe to be merged

    df_ip: pandas DataFrame
        The item price dataframe to be merged

    df_it: pandas DataFrame
        The item dataframe to be merged

    uid: str
        The name of the column containing the unique id

    Returns
    -------
    df: pandas DataFrame
        The merged dataframe containing sales, item price and item data.

    Example
    --------
    df = merge_data(df_sl, df_ip, df_it, uid)
    """

    df = (
        df_sl.merge(right=df_ip, on=[uid, time_index], how='left')
        .merge(right=df_it, on=uid, how='left')
    )

    return df


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
