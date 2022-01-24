import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
INSTANCE_NAME = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME', 'test-mysql')
INSTANCE_NAME2 = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME2', 'test-mysql2')
DB_NAME = os.environ.get('GCSQL_MYSQL_DATABASE_NAME', 'testdb')

def info():
    print('____________________________________')
    print('GCP_PROJECT_ID' + str(GCP_PROJECT_ID) )
    print('____________________________________')
    print('GCP_PROJECT_ID' + str(INSTANCE_NAME))
    print('____________________________________')
    print('GCP_PROJECT_ID' + str(INSTANCE_NAME2))
    print('____________________________________')
    print('GCP_PROJECT_ID' + str(DB_NAME))
    print('____________________________________')
    
dag = DAG('INFO-LOG', description='Hello World DAG',
              schedule_interval='0 12 * * *',
              start_date=datetime(2017, 3, 20), catchup=False)

info_op = PythonOperator(task_id='info', python_callable=info, dag=dag)

info_op
