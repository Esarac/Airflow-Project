from airflow import DAG, Dataset
from airflow.decorators import task

import logging
from datetime import date, datetime

my_file_1 = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

#Producer
with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023,1,1),
    catchup=False,
) as dag:
    @task(outlets=[my_file_1])
    def update_dataset_1():
        with open(my_file_1.uri, "a+") as f:
            f.write("file 1 update")
    
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("file 2 update")
    
    update_dataset_1() >> update_dataset_2()

#Consumer
with DAG(
    dag_id="consumer",
    schedule=[my_file_1, my_file_2],# As soon the dataset is updated, it will run the dag
    start_date=datetime(2023,1,1),
    catchup=False
) as dag:
    
    @task
    def read_datasets():
        with open(my_file_1.uri, "r") as f:
            logger = logging.getLogger("airflow.task")
            logger.info("file text: "+str("\t".join(f.readlines())))
        with open(my_file_2.uri, "r") as f:
            logger = logging.getLogger("airflow.task")
            logger.info("file text: "+str("\t".join(f.readlines())))
    
    read_datasets()