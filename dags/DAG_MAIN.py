from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from DW_TOOLS import create_connection, create_struct_db
from stg import *


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
    
    create_struct_db = PythonOperator(
        task_id='create_struct_db',
        python_callable=create_struct_db,
        op_kwargs={"con": con, "file": "dags/sql/DDL_STAGING_AREA.sql"},
        dag=dag)
    
    with TaskGroup(group_id='stages') as stages:
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
        
        stg_cid10_capitulos = PythonOperator(
            task_id='stg_cid10_capitulos',
            python_callable=run_cid10_capitulos,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cid10_capitulos'},
            dag=dag) 
        
        stg_cid10_grupos = PythonOperator(
            task_id='stg_cid10_grupos',
            python_callable=run_cid10_grupos,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cid10_grupos'},
            dag=dag) 
        
        stg_cid10_categorias = PythonOperator(
            task_id='stg_cid10_categorias',
            python_callable=run_cid10_categorias,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cid10_categorias'},
            dag=dag) 
        
        stg_cid10_subcategorias = PythonOperator(
            task_id='stg_cid10_subcategorias',
            python_callable=run_cid10_subcategorias,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cid10_subcategorias'},
            dag=dag) 
        
        stg_naturalidade = PythonOperator(
            task_id='stg_naturalidade',
            python_callable=run_stg_naturalidade,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_naturalidade',
                "sep": ','},
            dag=dag)
        
        stg_ocupacao = PythonOperator(
            task_id='stg_ocupacao',
            python_callable=run_stg_ocupacao,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_ocupacao',
                "sep": ','},
            dag=dag)
        
        stg_cbo = PythonOperator(
            task_id='stg_cbo',
            python_callable=run_stg_cbo,
            op_kwargs={
                'con': con,
                'schema': 'stg',
                'tb_name': 'stg_cbo'},
            dag=dag)       

    create_struct_db >> stages
