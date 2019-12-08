import os
from dask import compute
from pandas import Timestamp
from config import gcp_token
from parameters import import_merge_test_data, export_test_data
from app.ingest.import_merge import (
    import_gcs_data, merge_data, import_merge_data
)

# Set the GCP authentication credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_token


def test_import_gcs_data(import_merge_test_data=import_merge_test_data):
    """
    Unit test for the import_data function
    """

    # Function execution
    df_list = list(map(import_gcs_data, import_merge_test_data))
    df_list = compute(compute(df_list)[0])[0]

    # Tests
    assert df_list[0]['df_name'] == 'df_sl'
    assert df_list[1]['df_name'] == 'df_it'
    assert df_list[2]['df_name'] == 'df_ic'
    assert df_list[0]['df'].shape == (4, 3)
    assert df_list[1]['df'].shape == (4, 3)
    assert df_list[2]['df'].shape == (4, 2)
    assert df_list[0]['df']['item_id'][0] == 'A1'
    assert df_list[1]['df']['test_it'][1] == '0'
    assert df_list[1]['df']['test_it'][2] == '1'


def test_merge_data(import_merge_test_data=import_merge_test_data):
    """Unit test for the merge_data function"""

    # Function execution
    df_list = list(map(import_gcs_data, import_merge_test_data))
    df_list = compute(compute(df_list)[0])[0]
    df = compute(merge_data(df_list))[0]

    # Tests
    assert df.shape == (4, 6)
    assert isinstance(df['date'][0], Timestamp)
    assert df['test_sl'][0] == 20
    assert df['item_id'][1] == 'A2'
    assert df['date'][2] == Timestamp('01.01.1998')
    assert df['item_category_id'][3] == 4
    assert df['test_it'][0] == 'hello world'
    assert df['test_ic'][0] == 'Hello World'


def test_import_merge_data(
    import_merge_test_data=import_merge_test_data,
    export_test_data=export_test_data
):
    """Unit test for the import_merge_data function"""

    # Function execution
    df = import_merge_data(import_merge_test_data, export_test_data)

    # Tests
    assert df.shape == (4, 6)
    assert isinstance(df['date'][0], Timestamp)
    assert df['test_sl'][0] == 20
    assert df['item_id'][1] == 'A2'
    assert df['date'][2] == Timestamp('01.01.1998')
    assert df['item_category_id'][3] == 4
    assert df['test_it'][0] == 'hello world'
    assert df['test_ic'][0] == 'Hello World'
