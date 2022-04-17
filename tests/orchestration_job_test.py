from etl_job.load_job import load_data
from etl_job.transform_job import transform_data
from common_objects.constants import START_DATE, END_DATE
from etl_job.report_job import fill_report_table

# unit test for project
tasks_params = {'start_date': START_DATE, 'end_date': END_DATE}
load_data(**tasks_params)
transform_data(**tasks_params)
fill_report_table(**tasks_params)
