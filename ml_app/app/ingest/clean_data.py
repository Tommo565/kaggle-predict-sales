import pandas as pd
from app.utils.utils import reindex_by_date


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

    # Resample to monthly
    df_sl = (
        df_sl.set_index(time_index)
        .groupby(uid)
        .resample('M')
        .sum()
        .drop(uid, axis=1)
        .reset_index()
    )

    # Generate total monthly date range for the df
    dates = pd.date_range(
        start=df_sl[time_index].min(),
        end=df_sl[time_index].max(),
        freq='M'
    )

    # Infill missing months
    df_sl = (
        df_sl.set_index('date')
        .groupby('item_id')
        .apply(reindex_by_date, dates)
        .reset_index()
        .rename({'level_1': time_index}, axis=1)
    )

    return df_sl


def clean_item_price_data(df_ip, time_index, uid):
    """
    Summary
    -------
    Cleans and transforms the item price data contained in the sales dataframe.
    Returns this as a new dataframe.

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

    # Take the mean value of the overall item price data
    df_ip = (
        df_ip
        .set_index(time_index)
        .groupby(uid)
        .mean()
        .reset_index()
    )

    return df_ip


def merge_data(df_sl, df_ip, df_it, uid):
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
        df_sl.merge(right=df_ip, on=uid, how='left')
        .merge(right=df_it, on=uid, how='left')
    )

    return df
