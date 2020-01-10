import os
from config import gcp_token
from parameters import import_datasets_test
from app.ingest.import_data import (
    import_data, unpack_data
)


def test_unpack_data(import_datasets_test=import_datasets_test):
    """
    Unit test for the unpack_data function
    """

    df_list = list(map(import_data, import_datasets_test))
    df_sl, df_ip, df_it = unpack_data(df_list)

    # DataFrame structure tests
    assert df_sl.shape == (270, 3)
    assert df_ip.shape == (270, 3)
    assert df_it.shape == (2, 2)

    # DataFrame value tests
    assert len(df_sl['date'].unique().tolist()) == 90
    assert len(df_sl['item_id'].unique().tolist()) == 2
    assert df_sl['item_cnt_day'].sum() == 903

    assert len(df_ip['date'].unique().tolist()) == 90
    assert df_ip['item_id'].unique().tolist() == [0, 1]
    assert df_ip['item_price'].unique().tolist() == [350, 300, 250]
    assert df_ip['item_price'].sum() == 81000

    assert df_it['item_id'].unique().tolist() == [0, 1]
    assert df_it['item_category_id'].unique().tolist() == [500, 600]
