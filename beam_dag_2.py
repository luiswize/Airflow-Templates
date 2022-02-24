
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

default_args = {
    'owner': 'LuisMorales',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 17),
    'email': ['luis.morales@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('run_beam_pipelines',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


movies_pipeline_dataflow_runner = BeamRunPythonPipelineOperator(
    task_id="movies_review_job",
    runner="DataflowRunner",
    py_file='gs://codes-gcp-data-eng-appr04-cee96a91/movies_review.py',
    pipeline_options={
        'stagingLocation': 'gs://raw-layer-gcp-data-eng-appr04-cee96a91',
        'output': 'gs://staging-layer-gcp-data-eng-appr04-cee96a91',
        },
    py_requirements=['apache-beam[gcp]==2.26.0'],
    py_interpreter='python3',
    py_system_site_packages=False
)

movies_pipeline_dataflow_runner
