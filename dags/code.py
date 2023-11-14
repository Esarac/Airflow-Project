from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import json
from pandas import json_normalize

# Aux Functions
def _process_employee(ti):
    employee = ti.xcom_pull(task_ids="extract_employee")
    employee = employee['results'][0]
    processed_employee = json_normalize({
        'firstname': employee['name']['first'],
        'lastname': employee['name']['last'],
        'country': employee['location']['country'],
        'username': employee['login']['username'],
        'password': employee['login']['password'],
        'email': employee['email'],
    })
    processed_employee.to_csv('/tmp/processed_employee.csv', index=None, header=False)

def _store_employee():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = '''
            COPY Employee FROM stdin WITH DELIMITER AS ',';
        ''',
        filename='/tmp/processed_employee.csv',
    )

# DAG
with DAG('test_task', start_date=datetime(2023,1,1), schedule_interval='@daily',catchup=False) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS Employee(
                Firstname TEXT NOT NULL,
                Lastname TEXT NOT NULL,
                Country TEXT NOT NULL,
                Username TEXT NOT NULL,
                Password TEXT NOT NULL,
                Email TEXT NOT NULL
            );
        ''',
    )

    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
    )

    extract_employee = SimpleHttpOperator(
        task_id='extract_employee',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True #See response in UI
    )

    process_employee = PythonOperator(
        task_id='process_employee',
        python_callable=_process_employee
    )

    store_employee = PythonOperator(
        task_id='store_employee',
        python_callable=_store_employee
    )

    # Dependencies
    create_table >> is_api_available >> extract_employee >> process_employee >> store_employee