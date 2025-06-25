from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
from scripts import generate_data
from scripts.upload_to_blob import upload_to_blob_append

# Load environment variables
load_dotenv()

DATA_PATH = os.path.expanduser('~/venv/dags/data')
BLOB_CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# Validate environment variables early
if not BLOB_CONTAINER_NAME or not BLOB_CONNECTION_STRING:
    raise ValueError("Azure blob storage credentials not set properly in .env")

default_args = {
    'start_date': datetime(2023, 10, 1),
    'owner': 'airflow',
    'retries': 1
}


def delete_local_files(data_path):
    for filename in os.listdir(data_path):
        if filename.endswith('.csv'):
            os.remove(os.path.join(data_path, filename))
            print(f"Deleted: {filename}")

with DAG(
    dag_id='charity_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Charity dataset generation and blob upload DAG',
    tags=['charity', 'azure']
) as dag:

    generate_projects = PythonOperator(
        task_id='generate_projects',
        python_callable=generate_data.generate_projects,
        op_args=[os.path.join(DATA_PATH, 'projects.csv')]
    )

    generate_campaigns = PythonOperator(
        task_id='generate_campaigns',
        python_callable=generate_data.generate_campaigns,
        op_args=[os.path.join(DATA_PATH, 'campaigns.csv')]
    )

    generate_donations = PythonOperator(
        task_id='generate_donations',
        python_callable=generate_data.generate_donations,
        op_args=[os.path.join(DATA_PATH, 'donations.csv')]
    )

    generate_volunteers = PythonOperator(
        task_id='generate_volunteers',
        python_callable=generate_data.generate_volunteers,
        op_args=[os.path.join(DATA_PATH, 'volunteers.csv')]
    )

    generate_volunteer_shifts = PythonOperator(
        task_id='generate_volunteer_shifts',
        python_callable=generate_data.generate_volunteer_shifts,
        op_args=[os.path.join(DATA_PATH, 'volunteer_shifts.csv')]
    )

    generate_beneficiaries = PythonOperator(
        task_id='generate_beneficiaries',
        python_callable=generate_data.generate_beneficiaries,
        op_args=[os.path.join(DATA_PATH, 'beneficiaries.csv')]
    )

    generate_transactions = PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_data.generate_transactions,
        op_args=[os.path.join(DATA_PATH, 'transactions.csv')]
    )

    upload_to_blob = PythonOperator(
        task_id='upload_to_azure_blob',
        python_callable=upload_to_blob_append,
        op_kwargs={
            'data_path': DATA_PATH,
            'container_name': BLOB_CONTAINER_NAME,
            'connection_string': BLOB_CONNECTION_STRING
        }
    )

    delete_local_files_task = PythonOperator(
        task_id='delete_local_files',
        python_callable=delete_local_files,
        op_args=[DATA_PATH]
    )

    # Chain tasks explicitly and clearly
    generate_projects >> generate_campaigns >> generate_donations
    generate_donations >> generate_volunteers >> generate_volunteer_shifts
    generate_volunteer_shifts >> generate_beneficiaries >> generate_transactions
    generate_transactions >> upload_to_blob >> delete_local_files_task
