from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime

"""
Load GCS CSV file to a Postgres in GCP Cloud SQL Instance.
"""


default_args = {
    'owner': 'luis.morales',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 21),
    'email': ['luis.morales@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('load_data_gcs_to_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


def csv_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = pg_hook.get_conn()
    cur = get_postgres_conn.cursor()
    with open("user_purchase.csv", "r") as f:
        cur.copy_expert("COPY user_purchase FROM STDIN WITH CSV HEADER", f)
        get_postgres_conn.commit()
    cur.close()


task1 = PostgresOperator(task_id='create_postgres_table',
                         sql="""
                        CREATE TABLE IF NOT EXISTS user_purchase (    
                            invoice_number varchar(10),
                            stock_code varchar(20),
                            detail varchar(1000),
                            quantity int,
                            invoice_date timestamp,
                            unit_price numeric(8,3),
                            customer_id int,
                            country varchar(20)
                        );
                            """,
                         postgres_conn_id='postgres_default',
                         autocommit=True,
                         dag=dag)

task2 = GCSToLocalFilesystemOperator(task_id="download_gcs_file",
                                     bucket="data-apprenticeship",
                                     object_name="user_purchase.csv",
                                     filename="user_purchase.csv",
                                     gcp_conn_id="google_cloud_default",
                                     dag=dag)

task3 = PythonOperator(task_id='csv_to_database',
                       provide_context=True,
                       python_callable=csv_to_postgres,
                       dag=dag)

task1 >> task2 >> task3
