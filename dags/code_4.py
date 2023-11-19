from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from subdags.code_4_1 import subdag_downloads
from subdags.code_4_2 import subdag_transforms
 
from datetime import datetime
 
with DAG(
    dag_id='group_dag',
    start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily',
    catchup=False
) as dag:
    args = dag.__dict__
    
    download_files_dag_id = 'download_files'
    download_files = SubDagOperator(
        task_id = download_files_dag_id,
        subdag=subdag_downloads(dag.dag_id, download_files_dag_id, args)
    )
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_files_dag_id = 'transform_files'
    transform_files = SubDagOperator(
        task_id = transform_files_dag_id,
        subdag=subdag_transforms(dag.dag_id, transform_files_dag_id, args)
    )

    download_files >> check_files >> transform_files