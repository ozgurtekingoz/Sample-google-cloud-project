from etl_job.bigquery_service import execute_query, drop_table
from common_objects.queries import CLEAN_INVALID_RATE_CODES, CLEAN_ZERO_PASSENGERS, CLEAN_INVALID_LONGITUDE, \
    CLEAN_INVALID_LATITUDE, \
    CLEAN_INVALID_PAYMENT_TYPE, \
    CLEAN_INVALID_TRIP_TYPE, CLEAN_DUPLICATED_RECORDS, ENRICHMENT_QUERY
from common_objects.constants import PROJECT_ID, DATASET_ID, FILTERED_TABLE_NAME


# I did the cleanup by looking at the column descriptions of the original table.
def clear_erroneous_data(start_date_str, end_date_str):
    # All rate codes are deleted except 1,2,3,4,5,6
    query = CLEAN_INVALID_RATE_CODES.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                            START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("Rate codes are deleted")

    # deleted if the number of passengers is 0 or less
    query = CLEAN_ZERO_PASSENGERS.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                         START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("passenger_counts are deleted")

    # deleted if the pickup_longitude or dropoff_longitude is 0
    query = CLEAN_INVALID_LONGITUDE.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                           START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("pickup_longitudes/dropoff_longitudes are deleted")

    # deleted if the pickup_latitude or dropoff_latitude is 0
    query = CLEAN_INVALID_LATITUDE.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                          START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("pickup_latitudes/dropoff_longitudes are deleted")

    # All payment types are deleted except 1,2,3,4,5,6
    query = CLEAN_INVALID_PAYMENT_TYPE.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID,
                                              TABLE_ID=FILTERED_TABLE_NAME,
                                              START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("payment_types are deleted")

    # All trip_types are deleted except 1,2
    query = CLEAN_INVALID_TRIP_TYPE.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                           START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("trip_types are deleted")

    # Clean duplicated records
    query = CLEAN_DUPLICATED_RECORDS.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                            START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    drop_table(table_id="temp_dedup_data")
    print("duplicated records are deleted")


def enrich_tlc_data(start_date_str, end_date_str):
    # enrich with hexagons r9, day_parts
    query = ENRICHMENT_QUERY.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, TABLE_ID=FILTERED_TABLE_NAME,
                                    START_DATE=start_date_str, END_DATE=end_date_str)
    execute_query(query=query)
    print("enrichment steps are done")


def transform_data(**kwargs):
    start_date_str = kwargs.get('start_date')
    end_date_str = kwargs.get('end_date')
    clear_erroneous_data(start_date_str=start_date_str, end_date_str=end_date_str)
    enrich_tlc_data(start_date_str=start_date_str, end_date_str=end_date_str)
