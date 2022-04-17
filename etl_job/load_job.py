from common_objects.queries import TLC_2014_EXPORT_QUERY, TLC_2014_FILTERED_QUERY
from etl_job.bigquery_service import query_results_to_table, export_table_data_as_csv, drop_table, execute_query, \
    is_table_exists
from common_objects.constants import BUCKET_NAME, REPORT_DATE_FORMAT, FILTERED_TABLE_NAME, PROJECT_ID, DATASET_ID
import datetime


# create a partitioned table by filtering the data
def extract_tlc_data_bq(start_date_str, end_date_str):
    if is_table_exists(table_id=FILTERED_TABLE_NAME):
        drop_table(table_id=FILTERED_TABLE_NAME)

    query = TLC_2014_FILTERED_QUERY.format(START_DATE=start_date_str,
                                           END_DATE=end_date_str,
                                           PROJECT_ID=PROJECT_ID,
                                           DATASET_ID=DATASET_ID,
                                           TABLE_ID=FILTERED_TABLE_NAME)
    execute_query(query=query)


# create storage based on filtered table
# step1: move to records temp table
# step2: create storage files based on year,month, day
# step3: drop temp table

def extract_tlc_data_to_storage(start_date, end_date):
    daily_temp_table_name = 'temp_daily_tlc_data'
    while start_date <= end_date:
        report_date_str = start_date.strftime(REPORT_DATE_FORMAT)
        query = TLC_2014_EXPORT_QUERY.format(PROJECT_ID=PROJECT_ID,
                                             DATASET_ID=DATASET_ID,
                                             TABLE_ID=FILTERED_TABLE_NAME,
                                             REPORT_DATE=report_date_str)
        query_results_to_table(query=query, table_id=daily_temp_table_name)
        report_date_parts = str.split(report_date_str, '-')
        output_path = f'gs://{BUCKET_NAME}/{report_date_parts[0]}/{report_date_parts[1]}/{report_date_parts[2]}/tlc_daily_data.csv'
        export_table_data_as_csv(table_id=daily_temp_table_name, output_data_path=output_path)
        start_date = start_date + datetime.timedelta(days=1)

    drop_table(table_id=daily_temp_table_name)


def load_data(**kwargs):
    start_date_str = kwargs.get('start_date')
    end_date_str = kwargs.get('end_date')
    extract_tlc_data_bq(start_date_str=start_date_str, end_date_str=end_date_str)
    extract_tlc_data_to_storage(start_date=datetime.datetime.strptime(start_date_str, REPORT_DATE_FORMAT),
                                end_date=datetime.datetime.strptime(end_date_str, REPORT_DATE_FORMAT))
