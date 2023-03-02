from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from DW_TOOLS import create_connection, create_schema
from stg.STG_SIM import run_sim
from stg.STG_UF import run_stg_uf
from stg.STG_CBO import run_stg_cbo
from stg.STG_CID10 import run_cid10
from stg.STG_OCUPACAO import run_stg_ocupacao
from stg.STG_MUNICIPIOS import run_stg_municipios
from stg.STG_NATURALIDADE import run_stg_naturalidade


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
    
    with TaskGroup(group_id='stages') as stages:
        create_schema(con, "stg")

        stg_sim = PythonOperator(
            task_id='stg_sim',
            python_callable=run_sim,
            op_kwargs={
                'uf': 'ES',
                'start_year': 2010,
                'end_year': 2020,
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_sim'},
            dag=dag)
        
        stg_municipios = PythonOperator(
            task_id='stg_municipios',
            python_callable=run_stg_municipios,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_municipios'},
            dag=dag)
        
        stg_uf = PythonOperator(
            task_id='stg_uf',
            python_callable=run_stg_uf,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_uf'},
            dag=dag)
        
        stg_cid10 = PythonOperator(
            task_id='stg_cid10',
            python_callable=run_cid10,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cid10'},
            dag=dag) 
        
        stg_naturalidade = PythonOperator(
            task_id='stg_naturalidade',
            python_callable=run_stg_naturalidade,
            op_kwargs={
                'file_path': "dags/arquivos/tabNaturalidade.csv",
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_naturalidade'},
            dag=dag)
        
        stg_ocupacao = PythonOperator(
            task_id='stg_ocupacao',
            python_callable=run_stg_ocupacao,
            op_kwargs={
                'file_path': "dags/arquivos/tabOcupacao.csv",
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_ocupacao'},
            dag=dag)
        
        stg_cbo = PythonOperator(
            task_id='stg_cbo',
            python_callable=run_stg_cbo,
            op_kwargs={
                'file_path': "dags/arquivos/tabCBO.csv",
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cbo'},
            dag=dag)       
