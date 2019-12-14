import pandas as pd
from dask import delayed, compute


def resample_reindex_data(
    df, target_rename, target, time_index, all_columns,
    all_features_time_index, all_features, categorical_features
):
    """
    Summary
    -------
    Resamples a dataframe with a dailt DateTime index to monthly.

    Parameters
    ----------
    df: pandas.DataFrame
        The dataframe to be resampled.

    target: str
        The column name of the target to be predicted

    time_index: str
        The column name of the DateTime index
    categorical_features: list
        A list of features that are categorical
    numeric_features:
        A list of features that are numeric

    Returns
    -------
    df: pandas.Dataframe
        A pandas dataframe reindexed from daily to monthly

    Example
    --------
    df = resample_data(
        df, target_rename, target, time_index, all_columns,
        all_features_time_index, all_features, categorical_features
    )

    """

    # Group the dataframe and sum on the target to account for removed features
    df = (
        df.rename(target_rename, axis=1)
        [all_columns]
        .groupby(all_features_time_index)
        .sum()
        .fillna(0)
        .reset_index()
    )

    # Generate an index of the earliest and latest dates from the dataframe
    dates = pd.date_range(
        start=df[time_index].min(),
        end=df[time_index].max(),
        freq='M'
    )

    # Convert required columns to categorical
    for col in categorical_features:
        df[col] = df[col].astype('category')

    # Group the dataframe
    df_grouped = df.set_index(time_index).groupby(all_features)

    # Output Object to hold the results of the resample
    resample_output = []

    # Group the dataframe for iteration and resampling
    df_grouped = df.set_index(time_index).groupby(all_features)

    # Iterate through the groups and resample each
    for group in df_grouped.groups:
        df_gp = df_grouped.get_group(group)
        resample_dict = resample_to_monthly(df_gp, all_features)
        resample_output.append(resample_dict)

    # Run the Delayed function and convert to df
    resample_output = compute(resample_output)[0]

    # Unpack the output dict
    df_list = []

    # Create a list of records for conversion to a dataframe
    for item in resample_output:
        for record in item:
            df_list.append(record)

    # Create a new dataframe and reindex to include missing dates
    df = (
        pd.DataFrame(df_list)
        .set_index(time_index)
        .groupby(all_features)
        .apply(reindex_by_date, dates=dates)
        .drop(all_features, axis=1)
        .reset_index()
        .rename({'level_2': time_index}, axis=1)
    )

    return df


@delayed
def resample_to_monthly(df, all_features):
    """
    Summary
    -------
    Resamples a weekly dataframe to monthly. Outputs the dataframe as a
    dictionary of records.

    Delayed via Dask.

    Parameters
    ----------
    df: pandas.DataFrame
        A group within a dataframe to resample

    Returns
    -------
    resample_dict: dict
        A dictionary containing the dataframe resampled to monthly

    Example
    --------
    resample_dict = resample_to_monthly(df)
    """
    resample_dict = (
        df.groupby(all_features)
        .resample('M')
        .sum()
        .reset_index()
        .to_dict(orient='records')
    )
    return resample_dict


def reindex_by_date(df, dates):
    """
    Summary
    -------
    Generates a complete monthly DateTime index and reindexes a DateTime index
    on a pandas dataframe to add in missing date values. Assigns a 0 value to
    any fields which have are NaN.

    Should be applied to a dataframe via groupby.

    Sub-function used in the resample_data function.

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

    df = df.reindex(dates).fillna(0)
    return df
