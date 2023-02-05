from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from STG_SIM import Stg_SIM
from STG_SIH import Stg_SIH


with DAG(
    'stg_dag',
    start_date=datetime(2023, 2, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    conn_params = {
        'dbname': 'saude_mental',
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'schema': 'stg',
        'table_name': 'sim_2010_2020'
    }

    sim = PythonOperator(
        task_id='sim',
        python_callable=Stg_SIM.run(
            year_start=2010,
            year_end=2020,
            uf='ES',
            info_sys='SIM-DO',
            conn_params=conn_params
        )
    )

    sih = PythonOperator(
        task_id='sih',
        python_callable=Stg_SIH.run(
            year_start=2010,
            year_end=2020,
            uf='ES',
            info_sys='SIH-RD',
            conn_params=conn_params
        )
    )
