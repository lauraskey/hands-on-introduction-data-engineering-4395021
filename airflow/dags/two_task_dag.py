'''Two Task DAG'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'Laura',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2025,1,1)
}

with DAG(
    dag_id='two_task_dag',
    description='A two task Airflow DAG',
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:
    
    t0 = BashOperator(
        task_id='bash_task_0',
        bash_command='echo "First Airflow Task!"'
    )

    t1 = BashOperator(
        task_id='bash_task_1',
        bash_command='echo "Sleeping..." && sleep 5s && echo "Second Airflow task!"'
    )

    t0 >> t1
