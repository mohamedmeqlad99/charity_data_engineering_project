from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from dotenv import load_dotenv
import os
from scripts import generate_data
from scripts.upload_to_blob import upload_directory


load_dotenv()

DATA_PATH = os.getenv("DIR")
BLOB_CONTAINER_NAME = os.getenv("CONTAINER_NAME")
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if not BLOB_CONTAINER_NAME or not BLOB_CONNECTION_STRING:
    raise ValueError("Azure blob storage credentials not set properly in .env")

def delete_local_files(data_path):
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            os.remove(os.path.join(data_path, filename))
            print(f"Deleted: {filename}")

def build_generate_tasks(group, data_path):
    PythonOperator(
        task_id='generate_projects',
        python_callable=generate_data.generate_projects,
        op_args=[os.path.join(data_path, 'projects.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_campaigns',
        python_callable=generate_data.generate_campaigns,
        op_args=[os.path.join(data_path, 'campaigns.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_donations',
        python_callable=generate_data.generate_donations,
        op_args=[os.path.join(data_path, 'donations.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_volunteers',
        python_callable=generate_data.generate_volunteers,
        op_args=[os.path.join(data_path, 'volunteers.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_volunteer_shifts',
        python_callable=generate_data.generate_volunteer_shifts,
        op_args=[os.path.join(data_path, 'volunteer_shifts.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_beneficiaries',
        python_callable=generate_data.generate_beneficiaries,
        op_args=[os.path.join(data_path, 'beneficiaries.csv')],
        dag=group.dag,
        task_group=group,
    )
    PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_data.generate_transactions,
        op_args=[os.path.join(data_path, 'transactions.csv')],
        dag=group.dag,
        task_group=group,
    )

with DAG(
    dag_id='charity_data_pipeline',
    start_date=datetime(2023, 10, 1),
    schedule='@daily',
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
    description='Charity dataset generation and blob upload DAG',
    tags=['charity', 'azure']
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    with TaskGroup("generate_data_group") as generate_data_group:
        build_generate_tasks(generate_data_group, DATA_PATH)

    upload_task = PythonOperator(
        task_id='upload_to_azure_blob',
        python_callable=upload_directory,
        op_args=[DATA_PATH]
    )

    cleanup_task = PythonOperator(
        task_id='delete_local_files',
        python_callable=delete_local_files,
        op_args=[DATA_PATH]
    )

    start_pipeline >> generate_data_group >> upload_task >> cleanup_task
