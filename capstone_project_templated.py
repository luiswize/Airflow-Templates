from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator \
                        ,DataprocDeleteClusterOperator \
                        ,DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator



def csv_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = pg_hook.get_conn()
    cur = get_postgres_conn.cursor()
    with open("user_purchase.csv", "r") as f:
        cur.copy_expert("COPY user_purchase FROM STDIN WITH CSV HEADER", f)
        get_postgres_conn.commit()
    cur.close()
 
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    }
}

PYSPARK_MOVIES_JOB = {
    "reference": {"project_id": 'gcp-data-eng-appr04-cee96a91'},
    "placement": {"cluster_name": 'movies-review'},
    "pyspark_job": {"main_python_file_uri": 'gs://codes-gcp-data-eng-appr04-cee96a91/spark_movie_review.py'}
}

PYSPARK_LOGS_JOB = {
    "reference": {"project_id": 'gcp-data-eng-appr04-cee96a91'},
    "placement": {"cluster_name": 'logs-review'},
    "pyspark_job": {"main_python_file_uri": 'gs://codes-gcp-data-eng-appr04-cee96a91/Spark_log_reviews.py'}
}

default_args = {
    'owner': 'luis.morales',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 21),
    'email': ['luis.morales@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG("capstone_templated", 
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@once', 
    default_args=default_args,
    catchup=False  # Catchup  
    ) as dag:

    init = DummyOperator(task_id='init')
    # CREATE SCHEMA staging3;
    create_postgres = PostgresOperator(task_id='create_postgres_table',
                         sql="""
                        CREATE TABLE IF NOT EXISTS user_purchase (    
                            InvoiceNo varchar(10),
                            StockCode varchar(20),
                            Description varchar(1000),
                            Quantity int,
                            InvoiceDate timestamp,
                            UnitPrice numeric(8,3),
                            CustomerID int,
                            Country varchar(20)
                        );
                            """,
                         postgres_conn_id = '{{var.postgres_conn_id.val}}',
                         autocommit=True,
            )

    download_gcs_file = GCSToLocalFilesystemOperator(task_id="download_gcs_file",
                                     bucket="data-apprenticeship",
                                     object_name="user_purchase.csv",
                                     filename="user_purchase.csv",
                                     gcp_conn_id = '{{var.gcp_conn_id.val}}'  #####
                                    )

    csv_to_database = PythonOperator(task_id='csv_to_database',
                       provide_context=True,
                       python_callable=csv_to_postgres,   
                       )

    #-----------------CREATES CLUSTERS IN DATAPROC------------------

    create_movies_cluster = DataprocCreateClusterOperator(
        task_id="create_movies_cluster",
        project_id='{{ var.project_id.val }}',
        cluster_config=CLUSTER_CONFIG,
        cluster_name='movies-review', ########
        region = 'us-west1',
        use_if_exists = True,
        gcp_conn_id = '{{var.gcp_conn_id.val}}'   ########
    )

    create_log_cluster = DataprocCreateClusterOperator(
        task_id = "create_logs_cluster", 
        project_id = '{{ var.project_id.val }}',
        cluster_config = CLUSTER_CONFIG,
        cluster_name = 'logs-review',    ########
        region = 'us-west1',
        use_if_exists = True,
        gcp_conn_id = '{{var.gcp_conn_id.val}}'   ########
    )

    #-----------------SUBMIT DATAPROC JOBS------------------

    pyspark_movies_task = DataprocSubmitJobOperator(
        task_id="pysparkt_movies_task", 
        job = PYSPARK_MOVIES_JOB, 
        project_id='{{ var.project_id.val }}',
        region = 'us-west1' ,
        gcp_conn_id = '{{var.gcp_conn_id.val}}'   ######## 
    )

    pyspark_logs_task = DataprocSubmitJobOperator(
        task_id="pyspark_logs_task", 
        job = PYSPARK_LOGS_JOB, 
        project_id= '{{ var.project_id.val }}',
        region = 'us-west1' ,
        gcp_conn_id = '{{var.gcp_conn_id.val}}'   ########
    )

    #-----------------DESTROY CLUSTERS------------------

    delete_movies_cluster = DataprocDeleteClusterOperator(
        task_id="delete_movies_cluster", 
        project_id = '{{ var.project_id.val }}',
        region = 'us-west1', 
        cluster_name='movies-review' ,
        gcp_conn_id = '{{var.gcp_conn_id.val}}',    ########
        trigger_rule='all_done'
    )
    
    delete_logs_cluster = DataprocDeleteClusterOperator(
        task_id="delete_logs_cluster", 
        project_id = '{{ var.project_id.val }}',
        region = 'us-west1', 
        cluster_name='logs-review' ,
        gcp_conn_id = '{{var.gcp_conn_id.val}}',   ########
        trigger_rule='all_done'
    )

    copy_to_staging_layer = GCSToGCSOperator(
        task_id="to_staging_layer",
        source_bucket='data-apprenticeship',
        source_object="user_purchase.csv",
        destination_bucket = '{{var.destination_bucket.val}}', ########
        gcp_conn_id = '{{var.gcp_conn_id.val}}',
    )
    
       
    ## Here is supposed to be the creation of the cluster for the deeper analytics jobs. It is part of the next steps.
    ## Meanwhile I run it In my local
    cluster_analytics = DummyOperator(task_id='CreateClusterForAnalytics')
    
    ## Here is supposed to be the the deeper analytics job run. It is part of the next steps.
    ## Meanwhile I run it In my local https://github.com/luiswize/Milestone-Project/blob/main/analytics_jobs.ipynb
    analytics_job = DummyOperator(task_id='DataAnalysis')
    
    terminate = DummyOperator(task_id='DagTerminatedSuccesfully')

    
    #----------------- STEPS ------------------
    (
        init >> create_postgres >> download_gcs_file >> csv_to_database
    )
    (
        csv_to_database >> create_movies_cluster  >> pyspark_movies_task >> copy_to_staging_layer >> delete_movies_cluster >> cluster_analytics
             
    )
    (
        csv_to_database >> create_log_cluster >> pyspark_logs_task >> copy_to_staging_layer >> delete_logs_cluster  >> cluster_analytics
    )
    (
        cluster_analytics >> analytics_job >> terminate
    )

'''

    project_id='{{ var.project_id.val }}'
    postgres_conn_id = '{{var.postgres_conn_id.val}}'
    gcp_conn_id = '{{var.gcp_conn_id.val}}'
    destination_bucket = '{{var.destination_bucket.val}}'
    templates = {
        'project_id' : 'gcp-data-eng-appr04-cee96a91', ####
        "cluster_movies_name": 'movies-review',
        "cluster_logs_name": 'logs-review',
        'destination_bucket' : 'staging-layer-gcp-data-eng-appr04-cee96a91',
        'gcp_conn_id' : "google_cloud_default",
        'postgres_conn_id' : 'postgres_default'
    }

'''
