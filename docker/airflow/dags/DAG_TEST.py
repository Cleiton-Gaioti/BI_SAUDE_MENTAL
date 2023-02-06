from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator


cwd = '/opt/airflow/dags/scripts/'

with DAG(
    'stg_dag',
    start_date=datetime(2023, 2, 5),
    schedule_interval='@daily',
    catchup=False
) as dag:
    sim = BashOperator(
        task_id='SIM',
        # bash_command=f'{cwd}run_r.sh {cwd}sim.R ',
        bash_command=f'Rscript {cwd}sim.R ',
        dag=dag,
    )
