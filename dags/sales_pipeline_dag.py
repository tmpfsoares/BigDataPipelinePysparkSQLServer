from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

args = {'owner':'airflow', 'retries':2, 'retry_delay':timedelta(seconds=10)}

with DAG(dag_id='sales_pipeline_dag', default_args=args, start_date=datetime(2021,8,27,22,15), 
catchup=False, schedule_interval=timedelta(minutes=15), max_active_runs=1) as dag:
    
    t1 = BashOperator(task_id='load_hive_table', depends_on_past=False, bash_command='python3 /origin/pipeline_scripts/sales_pipeline_1.py')
    t2 = BashOperator(task_id='load_data_mart_tables', depends_on_past=False, bash_command='python3 /origin/pipeline_scripts/sales_pipeline_2.py')

    t1>>t2
