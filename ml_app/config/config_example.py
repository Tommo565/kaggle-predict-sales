import multiprocessing

# Local Config
local = 'Y'
local_export_folder = '../data/output_data'

# GCP Config
gcp_token = './config/gcp_token.json'
project_id = 'my_project'
bucket_name = 'my-bucket'
import_data_folder = 'my-import-data-folder'
export_data_folder = 'my-export-data-folder'

# BQ Config
bq_db = 'dev'

# Unit Test Config
test_bucket_name = 'my_test_bucket'
test_import_data_folder = 'my-test-import-data-folder'
test_export_data_folder = 'my-test-export-data-folder'

# Dask config
n_workers = multiprocessing.cpu_count()
threads_per_worker = 4
dashboard_address = ':1234'
