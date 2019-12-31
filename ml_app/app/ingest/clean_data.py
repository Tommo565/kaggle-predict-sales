import pandas as pd
from app.utils.utils import reindex_by_date


def clean_sales_data(df_sl, time_index, uid, target_rename):
    """

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

    """

    df_ip = (
        df_ip
        .set_index(time_index)
        .groupby(uid)
        .mean()
        .reset_index()
    )

    return df_ip


def merge_data(uid, df_sl, df_ip, df_it):
    """

    """

    df = (
        df_sl.merge(right=df_ip, on=uid, how='left')
        .merge(right=df_it, on=uid, how='left')
    )

    return df
