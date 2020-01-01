import pandas as pd
import numpy as np


def create_agg_features(
    df, time_index, all_columns, all_features, all_features_time_index
):
    """

    """

    df_fea = df.set_index(time_index).groupby(all_features).agg({
        'target': ['min', 'max', 'std', 'sum', 'mean', 'median']
    }).reset_index()
    df_fea.columns = ['_'.join(col) for col in df_fea.columns]

    # TODO: Solve this programatically
    df_fea = df_fea.rename({
        'item_id_': 'item_id',
        'item_category_id_': 'item_category_id',
        'item_price_': 'item_price'
    }, axis=1)

    df = pd.merge(
        left=df,
        right=df_fea,
        left_on=all_features,
        right_on=all_features,
        how='left'
    )

    # Generate year and month values
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    # Generate a sequential month variable
    df_seq = (
        pd.DataFrame(df['date'].unique())
        .reset_index()
        .rename({
            'index': 'sequence',
            0: time_index

        }, axis=1)
    )

    # Merge on Sequence data
    df = df.merge(right=df_seq, on=time_index, how='left')

    # Update feature data structures
    for col in df.columns.tolist():
        if (col in all_columns):
            pass

        else:
            all_columns.append(col)
            all_features.append(col)
            all_features_time_index.append(col)

            # TODO: Add append to numeric & categorical

    return df, all_columns, all_features, all_features_time_index
