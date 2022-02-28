from airflow import DAG
from datetime import datetime 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator \
                        ,DataprocDeleteClusterOperator \
                        ,DataprocSubmitJobOperator



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
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": 'gcp-data-eng-appr04-cee96a91'},
    "placement": {"cluster_name": 'movies-review'},
    "pyspark_job": {"main_python_file_uri": 'gs://codes-gcp-data-eng-appr04-cee96a91/spark_movie_review.py'}
}

with DAG("spark_jobs", 
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@once', 
    catchup=False  # Catchup  
    ) as dag:

    create_movies_cluster = DataprocCreateClusterOperator(
        task_id="create_movies_cluster",
        project_id='gcp-data-eng-appr04-cee96a91',
        cluster_config=CLUSTER_CONFIG,
        cluster_name='movies-review',
        region = 'us-west1',
        use_if_exists = True,
        gcp_conn_id = 'google_cloud_default'
    )

    pyspark_movies_task = DataprocSubmitJobOperator(
        task_id="pyspark-task", job=PYSPARK_JOB, project_id='gcp-data-eng-appr04-cee96a91',
            region = 'us-west1' ,gcp_conn_id = 'google_cloud_default'
    )

    delete_movies_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id='gcp-data-eng-appr04-cee96a91', region = 'us-west1', 
            cluster_name='movies-review' ,gcp_conn_id = 'google_cloud_default'
    )

    create_movies_cluster >> pyspark_movies_task >> delete_movies_cluster
