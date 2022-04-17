from common_objects.constants import PROJECT_ID, DATASET_ID, REPORT_TABLE_ID, FILTERED_TABLE_NAME, REPORT_TOP_N_RECORDS
from common_objects.queries import REPORT_DATA_QUERY
from etl_job.bigquery_service import is_table_exists, drop_table, execute_query


def fill_report_table(**kwargs):
    start_date_str = kwargs.get('start_date')
    end_date_str = kwargs.get('end_date')
    if is_table_exists(table_id=REPORT_TABLE_ID):
        drop_table(table_id=REPORT_TABLE_ID)

    query = REPORT_DATA_QUERY.format(START_DATE=start_date_str, END_DATE=end_date_str, PROJECT_ID=PROJECT_ID,
                                     DATASET_ID=DATASET_ID,
                                     TABLE_ID=FILTERED_TABLE_NAME, REPORT_TABLE_ID=REPORT_TABLE_ID,
                                     THRESHOLD=REPORT_TOP_N_RECORDS)

    execute_query(query=query)
