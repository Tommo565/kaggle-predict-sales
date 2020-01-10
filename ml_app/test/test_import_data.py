import os
from config import gcp_token
from parameters import import_datasets_test
from app.ingest.import_data import (
    import_data, unpack_data
)

# Set the GCP authentication credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token


def test_import_data(import_datasets_test=import_datasets_test):
    """
    Unit test for the import_data function
    """

    df_list = list(map(import_data, import_datasets_test))

    print(df_list)

    # Data Structure tests
    assert isinstance(df_list, list)
    assert df_list[0]['df_name'] == 'df_sl'
    assert df_list[1]['df_name'] == 'df_it'
    assert df_list[2]['df_name'] == 'df_ic'

    # Unpack dataframes
    df_sl = df_list[0]['df']
    df_it = df_list[1]['df']
    df_ic = df_list[2]['df']

    # DataFrame structure tests
    assert df_sl.shape == (270, 6)
    assert df_it.shape == (2, 3)
    assert df_ic.shape == (2, 2)

    assert len(df_sl['date'].unique().tolist()) == 90
    assert len(df_sl['item_id'].unique().tolist()) == 2
    assert len(df_it['item_id'].unique().tolist()) == 2
    assert len(df_it['item_category_id'].unique().tolist()) == 2
    assert len(df_ic['item_category_id'].unique().tolist()) == 2

    # DataFrame value tests
    assert df_sl['date_block_num'].unique().tolist() == [1, 2, 3]
    assert df_sl['item_id'].unique().tolist() == [0, 1]
    assert df_sl['shop_id'].unique().tolist() == [295, 300]
    assert df_sl['item_price'].unique().tolist() == [350, 300, 250]
    assert df_sl['item_cnt_day'].sum() == 903

    assert df_it['item_name'].unique().tolist() == ['Test_0', 'Test_1']
    assert df_it['item_id'].unique().tolist() == [0, 1]
    assert df_it['item_category_id'].unique().tolist() == [500, 600]

    assert df_ic['item_category_name'].unique().tolist() == (
        ['Category 500', 'Category 600']
    )
    assert df_ic['item_category_id'].unique().tolist() == [500, 600]
