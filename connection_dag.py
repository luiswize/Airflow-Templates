
from airflow import DAG
from datetime import datetime
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSqlInstanceImportOperator

#CloudSqlInstanceImportOperator
def yeah():
    print('yeah this is cool')
# raw_data = pd.read_csv('https://storage.googleapis.com/resources_data_eng_app04/user_purchase.csv')


import_body = {"importContext": {
    "fileType": "csv",
    "uri": 'gs://resources_data_eng_app04/user_purchase.csv',
    "database": 'dbname',
    "csvImportOptions": {
    "table": 'apprenticeship.user_purchase',
        },
    "importUser": 'dbuser'
    }}

with DAG(dag_id="postgres_operator_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@once", catchup=False) as dag:
    
    sql_import_task = CloudSqlInstanceImportOperator(
        task_id='sql_import_task',
        project_id='gcp-data-eng-appr04-cee96a91',
        body = import_body,
        instance='sql-milestone-5',
        gcp_conn_id='my_gcp_connection'
    )

    t2 = PythonOperator(
        task_id='yeah',
        python_callable=yeah
    )

    sql_import_task >> t2
