import pandas as pd
from dask import delayed, compute


def create_lag_features(df_fea, df_ts, uid, time_index, cols_to_lag):
    """
    Summary
    -------
    Merges time series and aggregate features DataFrames and then lags the
    relevant columns specified in the cols to lag parameter via the pandas
    shift method by 1, 2 or 3 months.

    Lagged data is then merged onto the merged time series and aggregate
    features DataFrame.

    Parameters
    ----------
    df_fea: pandas.DataFrame
        A DataFrame containing aggregate features data.

    df_ts:
        A DataFrame containing time series data.

    uid: str
        The name of the column containing the unique id

    time_index: str
        The name of the column containing the time index

    cols_to_lag: str
        A list of column names to lag.

    Returns
    -------
    df: pandas.DataFrame
        A DataFrame containing the lagged colums merged with the input
        DataFrame

    Example
    --------
    df = create_lag_features(df_fea, df_ts, uid, time_index, cols_to_lag)

    """

    # Merge the features and time series datasets
    df = pd.merge(left=df_fea, right=df_ts, on=[time_index, uid], how='outer')

    # Group the DataFrame
    df_lag_gp = (
        df[[time_index, uid] + cols_to_lag]
        .groupby(uid)
    )

    # Create an ontput list to append a result to
    lag_output = []

    # Generate a lag for each record
    for group in df_lag_gp.groups:
        df_ind = df_lag_gp.get_group(group)
        df_ind = lag_columns(df_ind, time_index, cols_to_lag)
        lag_output.append(df_ind)

    # Compute the lag via Dask
    lag_output = compute(lag_output)[0]

    # Create an empty list to append results to
    records_list = []

    # Create a list of records for conversion to a DataFrame
    for item in lag_output:
        for record in item:
            records_list.append(record)

    # Convert the output list of records into a DataFrame
    df_lag = pd.DataFrame(records_list)

    # Merge on to existing features
    df = pd.merge(left=df, right=df_lag, on=[uid, time_index], how='outer')

    return df


@delayed
def lag_columns(df, time_index, cols_to_lag):
    """
    Summary
    -------
    Lags the columns specified in the cols to lag parameter via the pandas
    shift method by 1, 2 or 3 months.

    Delayed via Dask.

    Applied to a pandas DataFrame via apply.

    Parameters
    ----------

    df: pandas.DataFrame
        A DataFrame containing an individual uid record

    cols_to_lag: str
        A list of column names to lag

    time_index: str
        The name of the column containing the time index

    Returns
    -------
    df: pandas.DataFrame
        A DataFrame containing the lagged colums

    Example
    --------
    for group in df_lag_gp.groups:
        df_ind = df_lag_gp.get_group(group)
        df_ind = lag_columns(df_ind, cols_to_lag)
        lag_output.append(df_ind)

    """
    df_copy = df.copy()
    df_copy.set_index(time_index)

    for col in cols_to_lag:

        df_copy[f'{col}_lag1'] = df[col].copy().shift(periods=1)
        df_copy[f'{col}_lag2'] = df[col].copy().shift(periods=2)
        df_copy[f'{col}_lag3'] = df[col].copy().shift(periods=3)
        df_copy.reset_index()

        # Remove the first 3 records
        df_copy = df_copy[3:]

    return df_copy.to_dict(orient='records')
