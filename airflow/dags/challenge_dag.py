'''Challenge DAG'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

default_args = {
    'owner': 'Laura',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    dag_id='challenge_dag',
    description='Challenge DAG',
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False
) as dag:

    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/constituents.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-extract-data.csv'
    )

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-extract-data.csv')
        result_df = df.groupby(['Sector'])['Sector'].count().reset_index(name='Count')
        result_df["Date"] = today.strftime("%Y-%m-%d")
        result_df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-transform-data.csv', index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)
    
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-transform-data.csv sp_500_sector_count" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-load-db.db',
        dag=dag
    )
    
    extract_task >> transform_task >> load_task
