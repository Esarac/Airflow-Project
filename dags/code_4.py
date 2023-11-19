from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from groups.code_4_1 import download_tasks
from groups.code_4_2 import transform_tasks
 
from datetime import datetime
 
with DAG(
    dag_id='group_dag',
    start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily',
    catchup=False
) as dag:
    args = dag.__dict__
    
    download_files = download_tasks()
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_files = transform_tasks()

    download_files >> check_files >> transform_files