from airflow import DAG
from datetime import datetime 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator \
                        ,DataprocDeleteClusterOperator \
                        ,DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator



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

with DAG("spark_jobs", 
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@once', 
    catchup=False  # Catchup  
    ) as dag:

    #-----------------UPLOAD FILES TO GCP------------------
    upload_logs = LocalFilesystemToGCSOperator(
        task_id = 'upload_log_csv',
        src = '/Users/luis.morales/Desktop/Test-env/raw/log_reviews.csv',
        dst = 'gs://raw-layer-gcp-data-eng-appr04-cee96a91/',
        bucket = 'raw-layer-gcp-data-eng-appr04-cee96a91',
        gcp_conn_id = 'google_cloud_storage'
    )

    upload_movies = LocalFilesystemToGCSOperator(
        task_id = 'upload_log_csv',
        src = '/Users/luis.morales/Desktop/Test-env/raw/movie_review.csv',
        dst = 'gs://raw-layer-gcp-data-eng-appr04-cee96a91/',
        bucket = 'raw-layer-gcp-data-eng-appr04-cee96a91',
        gcp_conn_id = 'google_cloud_storage'
    )

        #-----------------UPLOAD CODES TO GCP------------------
    upload_logs_code = LocalFilesystemToGCSOperator(
        task_id = 'upload_spark_log_Reviews.py',
        src = '/Users/luis.morales/Desktop/Test-env/Spark_log_reviews.py',
        dst = 'gs://codes-gcp-data-eng-appr04-cee96a91/',
        bucket = 'codes-gcp-data-eng-appr04-cee96a91',
        gcp_conn_id = 'google_cloud_storage'
    )

    upload_movies_code = LocalFilesystemToGCSOperator(
        task_id = 'upload_spark_movie_review.py',
        src = '/Users/luis.morales/Desktop/Test-env/spark_movie_review.py',
        dst = 'gs://codes-gcp-data-eng-appr04-cee96a91/',
        bucket = 'codes-gcp-data-eng-appr04-cee96a91',
        gcp_conn_id = 'google_cloud_storage'
    )
    
    #-----------------CREATES CLUSTERS IN DATAPROC------------------

    create_movies_cluster = DataprocCreateClusterOperator(
        task_id="create_movies_cluster",
        project_id='gcp-data-eng-appr04-cee96a91',
        cluster_config=CLUSTER_CONFIG,
        cluster_name='movies-review',
        region = 'us-west1',
        use_if_exists = True,
        gcp_conn_id = 'google_cloud_default'
    )

    create_log_cluster = DataprocCreateClusterOperator(
        task_id = "create_logs_cluster",
        project_id = 'gcp-data-eng-appr04-cee96a91',
        cluster_config = CLUSTER_CONFIG,
        cluster_name = 'logs-review',
        region = 'us-west1',
        use_if_exists = True,
        gcp_conn_id = 'google_cloud_default'
    )

    #-----------------SUBMIT DATAPROC JOBS------------------

    pyspark_movies_task = DataprocSubmitJobOperator(
        task_id="pysparkt_movies_task", 
        job = PYSPARK_MOVIES_JOB, 
        project_id='gcp-data-eng-appr04-cee96a91',
        region = 'us-west1' ,
        gcp_conn_id = 'google_cloud_default'
    )

    pyspark_logs_task = DataprocSubmitJobOperator(
        task_id="pyspark_logs_task", 
        job = PYSPARK_LOGS_JOB, 
        project_id='gcp-data-eng-appr04-cee96a91',
        region = 'us-west1' ,
        gcp_conn_id = 'google_cloud_default'
    )

    #-----------------DESTROY CLUSTERS------------------

    delete_movies_cluster = DataprocDeleteClusterOperator(
        task_id="delete_movies_cluster", 
        project_id='gcp-data-eng-appr04-cee96a91', 
        region = 'us-west1', 
        cluster_name='movies-review' ,
        gcp_conn_id = 'google_cloud_default'
    )
    
    delete_logs_cluster = DataprocDeleteClusterOperator(
        task_id="delete_logs_cluster", 
        project_id='gcp-data-eng-appr04-cee96a91', 
        region = 'us-west1', 
        cluster_name='logs-review' ,
        gcp_conn_id = 'google_cloud_default'
    )

    upload_logs >> upload_movies >> [upload_logs_code, upload_movies_code] >> create_movies_cluster >> create_log_cluster >> [pyspark_movies_task, pyspark_logs_task] \
        >> delete_movies_cluster >> delete_logs_cluster
