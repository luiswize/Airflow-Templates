from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLCreateInstanceDatabaseOperator
from airflow.operators.python import PythonOperator

db_create_body = {"instance": '2ndMilestone', "name": 'user_purchase', "project": 'gcp-data-eng-appr04-cee96a91'}

def yeah():
    print('the db was created cool')

with DAG(dag_id="postgres_operator_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@once", catchup=False) as dag:

    sql_db_create_task = CloudSQLCreateInstanceDatabaseOperator(
        project_id='gcp-data-eng-appr04-cee96a91',
        body=db_create_body, instance='posgress_db_test',
        task_id='sql_db_create_task'
    )

    t2 = PythonOperator(
        task_id='yeah_task',
        python_callable=yeah
    )

    sql_db_create_task >> t2


