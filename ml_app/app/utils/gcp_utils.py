import dask.dataframe as dd
import gcsfs
import google.auth
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1


def gcp_create_credentials(token):
    """
    Creates a credentials object from your GCP token to allow access to GCP
    services.
    """
    credentials = service_account.Credentials.from_service_account_file(token)

    return credentials


def gcs_file_import(
    filename, folder, project_id, bucket_name, token, delimiter=',',
    sheet_name=0, header=0
):
    """
    Imports a file from GCS storage and creates a pandas dataframe.
    Can handle csv, xls and xlsx files
    """

    # Connect to GCS using the token
    print(f'  - Importing {filename} from GCS')
    fs = gcsfs.GCSFileSystem(
        project=project_id,
        token=token
    )
    # Open the file
    with fs.open(f'gs://{bucket_name}/{folder}/{filename}') as f:

        # If excel
        if (filename[-5:] == '.xlsx') | (filename[-4:] == '.xls'):
            df = dd.read_excel(f, sheet_name=sheet_name, header=header)

        # If csv
        if (filename[-4:] == '.csv'):
            df = dd.read_csv(f, delimiter=delimiter)

    return df


def gcs_file_export(bucket_name, source_file_name, path):
    """
    Exports a file to a GCP storage bucket
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(path)

    blob.upload_from_filename(source_file_name)

    print(f'    - File {source_file_name} uploaded to GCS.')


def bq_table_import(token, query_string, credentials):
    """
    Imports a table from Bigquery based upon the input parameters.
    """

    # Create Clients
    bqclient = bigquery.Client()
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
        credentials=credentials
    )

    # Retrieve BQ data
    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )

    return df


def bq_table_export(df, token, credentials, project_id, bq_database, bq_table):
    """
    Exports a pandas dataframe to BigQuery
    """

    # Write DF to BigQuery
    df.to_gbq(
        destination_table=f'{bq_database}.{bq_table}',
        project_id=project_id,
        if_exists='replace',
        progress_bar=False,
        credentials=credentials
    )
    print(f'    - Table {bq_database}.{bq_table} Created in BigQuery')
