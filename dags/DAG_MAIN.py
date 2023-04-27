from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from dw import *
from stg import *
import DW_TOOLS as dwt


with DAG(
    'DAG_MAIN',
    start_date=datetime(2023, 2, 25),
    schedule_interval='@daily',
    concurrency=3,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}
) as dag:
    con = dwt.create_connection(
        server='172.19.112.1', #'10.3.152.103', 
        database='saude_mental', 
        username='postgres', 
        password='postgres', 
        port=5432)
    
    with TaskGroup(group_id='create_struct_db') as struct_db:
        stg_schema = "stg"

        create_struct_stg = PythonOperator(
            task_id='create_struct_stg',
            python_callable=dwt.create_struct_db,
            op_kwargs={"con": con, "file": "dags/sql/DDL_STAGING_AREA.sql"},
            dag=dag)

        create_struct_dw = PythonOperator(
            task_id='create_struct_dw',
            python_callable=dwt.create_struct_db,
            op_kwargs={"con": con, "file": "dags/sql/DDL_DW.sql"},
            dag=dag)

    with TaskGroup(group_id='stages') as stages:
        stg_sim = PythonOperator(
            task_id='stg_sim',
            python_callable=run_sim,
            op_kwargs={
                'ufs': ['ES'],
                'start_year': 2010,
                'end_year': 2020,
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_sim'},
            dag=dag)
        
        stg_municipios = PythonOperator(
            task_id='stg_municipios',
            python_callable=run_stg_municipios,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_municipios'},
            dag=dag)
        
        stg_cid10_capitulos = PythonOperator(
            task_id='stg_cid10_capitulos',
            python_callable=run_cid10_capitulos,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_cid10_capitulos'},
            dag=dag) 
        
        stg_cid10_categorias = PythonOperator(
            task_id='stg_cid10_categorias',
            python_callable=run_cid10_categorias,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_cid10_categorias'},
            dag=dag) 
        
        stg_cid10_subcategorias = PythonOperator(
            task_id='stg_cid10_subcategorias',
            python_callable=run_cid10_subcategorias,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_cid10_subcategorias'},
            dag=dag)
        
        stg_uf = PythonOperator(
            task_id='stg_uf',
            python_callable=run_stg_uf,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_uf'},
            dag=dag)
        
        stg_pais = PythonOperator(
            task_id='stg_pais',
            python_callable=run_stg_pais,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_pais'},
            dag=dag)
        
        stg_ocupacao = PythonOperator(
            task_id='stg_ocupacao',
            python_callable=run_stg_ocupacao,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_ocupacao',
                "sep": ','},
            dag=dag)
        
        stg_cbo = PythonOperator(
            task_id='stg_cbo',
            python_callable=run_stg_cbo,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_cbo'},
            dag=dag)


    with TaskGroup(group_id='dimensoes') as dimensoes:
        dw_schema = 'dw'

        d_municipio = PythonOperator(
            task_id='d_municipio',
            python_callable=run_d_municipio,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_municipio'},
            dag=dag)

        d_naturalidade = PythonOperator(
            task_id='d_naturalidade',
            python_callable=run_d_naturalidade,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_naturalidade'},
            dag=dag)

        d_ocupacao = PythonOperator(
            task_id='d_ocupacao',
            python_callable=run_d_ocupacao,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_ocupacao'},
            dag=dag)

        d_raca_cor = PythonOperator(
            task_id='d_raca_cor',
            python_callable=run_d_raca_cor,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_raca_cor'},
            dag=dag)

        d_cid = PythonOperator(
            task_id='d_cid',
            python_callable=run_d_cid,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_cid'},
            dag=dag)

        d_escolaridade = PythonOperator(
            task_id='d_escolaridade',
            python_callable=run_d_escolaridade,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_escolaridade'},
            dag=dag)

        d_sexo = PythonOperator(
            task_id='d_sexo',
            python_callable=run_d_sexo,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_sexo'},
            dag=dag)

    f_obito = PythonOperator(
        task_id='f_obito',
        python_callable=run_f_obito,
        op_kwargs={
            'con': con,
            'schema': dw_schema,
            'tb_name': 'f_obito'},
        dag=dag)

    struct_db >> stages >> dimensoes >> f_obito
