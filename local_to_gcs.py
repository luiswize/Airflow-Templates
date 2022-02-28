from airflow import DAG
from datetime import datetime 
from airflow.models import BaseOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

with DAG("prework", 
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@once', 
    catchup=False  # Catchup  
    ) as dag:
    #-----------------UPLOAD FILES TO GCP------------------
    upload_logs = LocalFilesystemToGCSOperator(
        task_id = 'upload_log_csv',
        src = '/Users/luis.morales/Documents/La_Salle/Budget.xlsx',
        dst = 'gs://raw-layer-gcp-data-eng-appr04-cee96a91',
        bucket = 'raw-layer-gcp-data-eng-appr04-cee96a91',
        gcp_conn_id = 'google_cloud_storage'
    )

    # upload_movies = LocalFilesystemToGCSOperator(
    #     task_id = 'upload_movies_csv',
    #     src = '/Users/luis.morales/Desktop/Test-env/raw/movie_review.csv',
    #     dst = 'gs://raw-layer-gcp-data-eng-appr04-cee96a91/',
    #     bucket = 'raw-layer-gcp-data-eng-appr04-cee96a91',
    #     gcp_conn_id = 'google_cloud_storage'
    # )

    #     #-----------------UPLOAD CODES TO GCP------------------
    # upload_logs_code = LocalFilesystemToGCSOperator(
    #     task_id = 'upload_spark_log_Reviews.py',
    #     src = '/Users/luis.morales/Desktop/Test-env/Spark_log_reviews.py',
    #     dst = 'gs://codes-gcp-data-eng-appr04-cee96a91/',
    #     bucket = 'codes-gcp-data-eng-appr04-cee96a91',
    #     gcp_conn_id = 'google_cloud_storage'
    # )

    # upload_movies_code = LocalFilesystemToGCSOperator(
    #     task_id = 'upload_spark_movie_review.py',
    #     src = '/Users/luis.morales/Desktop/Test-env/spark_movie_review.py',
    #     dst = 'gs://codes-gcp-data-eng-appr04-cee96a91/',
    #     bucket = 'codes-gcp-data-eng-appr04-cee96a91',
    #     gcp_conn_id = 'google_cloud_storage'
    # )
    upload_logs
