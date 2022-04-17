# import os
#
# os.environ[
#     'GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/ozgur/PycharmProjects/ozgur-configuration/valued-odyssey-347112-ef8da18892d7.json'

from google.cloud import bigquery
from common_objects.constants import PROJECT_ID, DATASET_ID
from google.cloud.exceptions import NotFound

client = bigquery.Client(project=PROJECT_ID)


def execute_query(query):
    client.query(query=query).result()


def query_results_to_table(query, table_id):
    table_name = f'{PROJECT_ID}.{DATASET_ID}.{table_id}'
    job_config = bigquery.QueryJobConfig(destination=table_name)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_name))


def export_table_data_as_csv(table_id, output_data_path):
    """
    :param table_id: "your-project.your_dataset.your_table_name"
    :param output_data_path: gs://your-bucket-name/your-path/file_name.csv
    :return:
    """

    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        output_data_path,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print("Exported {}:{}.{} to {}".format(PROJECT_ID, DATASET_ID, table_id, output_data_path))


def drop_table(table_id):
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    table_name = f'{PROJECT_ID}.{DATASET_ID}.{table_id}'
    client.delete_table(table_name, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_name))


def is_table_exists(table_id):
    table_name = f'{PROJECT_ID}.{DATASET_ID}.{table_id}'
    try:
        client.get_table(table_name)  # Make an API request.
        return True
    except NotFound:
        return False
