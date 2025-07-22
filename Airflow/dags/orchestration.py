from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from scripts.download_kaggle import download_kaggle_files
from scripts.download_unemployment import download_unemployment
from scripts.ingest_kaggle import upload_kaggle_files
from scripts.ingest_gov import upload_gov_files

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='Transaction_Pipeline',
    default_args=default_args,
    catchup=False,
    tags=['Python', 'dbt', 'AWS'],
    max_active_runs=1,
) as dag:

    download_kaggle = PythonOperator(
        task_id='download_kaggle_files',
        python_callable=download_kaggle_files
    )

    download_unrate = PythonOperator(
        task_id='download_unemployment',
        python_callable=download_unemployment
    )

    upload_kaggle = PythonOperator(
        task_id='upload_kaggle',
        python_callable=upload_kaggle_files
    )

    upload_gov = PythonOperator(
        task_id='upload_gov',
        python_callable=upload_gov_files
    )

    download_kaggle >> download_unrate >> upload_kaggle >> upload_gov
