from airflow import DAG
""" from main import run_sim
from airflow.operators.python import PythonOperator
from STG_SIM import Stg_SIM """
from datetime import datetime
from CONNECTION import create_connection
from airflow.operators.bash import BashOperator


cwd = '/opt/airflow/dags/scripts/'

with DAG(
    'stg_dag',
    start_date=datetime(2023, 2, 5),
    schedule_interval='@daily',
    catchup=False
) as dag:
    con = create_connection(
        server='localhost', 
        database='saude_mental', 
        username='postgres', 
        password='postgres', 
        port=5432)

    sim = BashOperator(
        task_id='SIM',
        # bash_command=f'{cwd}run_r.sh {cwd}sim.R ',
        bash_command=f'Rscript {cwd}sim.R',
        dag=dag,
    )
    
    """sim = PythonOperator(
        task_id='sim',
        python_callable=run_sim(
            uf='ES',
            year_start=2010,
            year_end=2020,
            con=con,
            schema='stg',
            tb_name='sim_2010_2020'
        )
    )"""
