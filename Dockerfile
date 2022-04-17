FROM python:3.8-slim

RUN apt-get update && apt-get install -y supervisor && \
    yes | apt-get install gcc python3-dev

ENV AIRFLOW_HOME=/ozgur/airflow

# You can uncomment the following line to disable the authentication
# ENV AUTH_ROLE_PUBLIC = 'Admin'

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY . /ozgur/ozgur_tekingoz_firefly_case

ENV GOOGLE_APPLICATION_CREDENTIALS=/ozgur/ozgur_tekingoz_firefly_case/gcp-service-account.json

RUN pip3 install --upgrade pip && \
    pip3 install -r /ozgur/ozgur_tekingoz_firefly_case/requirements.txt && \
    pip3 install SQLAlchemy==1.3.23 && \
    pip3 install Flask-SQLAlchemy==2.4.4

RUN mkdir /ozgur/airflow && mkdir /ozgur/airflow/dags && mkdir /ozgur/airflow/dags/etl_job && mkdir /ozgur/airflow/dags/common_objects
RUN cp /ozgur/ozgur_tekingoz_firefly_case/etl_job/bigquery_service.py /ozgur/airflow/dags/etl_job/bigquery_service.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/common_objects/constants.py /ozgur/airflow/dags/common_objects/constants.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/etl_job/load_job.py /ozgur/airflow/dags/etl_job/load_job.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/common_objects/queries.py /ozgur/airflow/dags/common_objects/queries.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/etl_job/report_job.py /ozgur/airflow/dags/etl_job/report_job.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/etl_job/transform_job.py /ozgur/airflow/dags/etl_job/transform_job.py
RUN cp /ozgur/ozgur_tekingoz_firefly_case/orchestration_job.py /ozgur/airflow/dags/orchestration_job.py

RUN airflow db init && \
    airflow users create --role Admin --username ozgur --email ozgur@ozgur.com --firstname Ozgur --lastname Tekingoz --password ozgur


EXPOSE 8080

CMD ["/usr/bin/supervisord"]