import pandas as pd
from airflow import DAG
from datetime import datetime
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# raw_data = pd.read_csv('https://storage.googleapis.com/resources_data_eng_app04/user_purchase.csv')
with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_user_purchase_table = PostgresOperator(
        task_id = 'create_user_purchase_table',
        sql = """
        CREATE SCHEMA Milestone;
        CREATE TABLE Milestone.user_purchase(
            invoice_number varchar(10),
            stock_code varchar(20),
            detail varchar(1000),
            quantity int,
            invoide_date timestamp,
            unit_price numeric(8,3),
            customer_id int,
            country varchar(20)
        ); 
        """
    )
