from airflow import DAG
from stg.STG_SIM import run_sim
from datetime import datetime
from DW_TOOLS import create_connection
from airflow.operators.python import PythonOperator


cwd = '/opt/airflow/dags/scripts/'

with DAG(
    'DAG_MAIN',
    start_date=datetime(2023, 2, 25),
    schedule_interval='@daily',
    catchup=False
) as dag:
    con = create_connection(
        server='10.3.152.103', 
        database='saude_mental', 
        username='postgres', 
        password='postgres', 
        port=5432)
    
    sim = PythonOperator(
        task_id='sim',
        python_callable=run_sim,
        op_kwargs={
            'uf': 'ES',
            'start_year': 2010,
            'end_year': 2020,
            'con': con,
            'schema': 'stg',
            'tb_name': 'sim_2010_2020'},
        dag=dag
    )
