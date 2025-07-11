'''One Task DAG'''
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
    dag_id='one_task_dag',
    description='A one task Airflow DAG',
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:
    
    task1 = BashOperator(
        task_id='one_task',
        bash_command='echo "hello linkedin learning!" > /workspaces/hands-on-introduction-data-engineering-4395021/lab/create_this_file.txt',
        dag=dag
    )
