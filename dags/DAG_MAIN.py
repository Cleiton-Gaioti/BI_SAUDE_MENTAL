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
    concurrency=1,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}
) as dag:
    
    ufs = ['ES']
    start_year = 2010
    end_year = 2021

    con = dwt.create_connection(
        server='10.3.152.103', 
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
        
        stg_tb_sigtab = PythonOperator(
            task_id='stg_tb_sigtab',
            python_callable=run_stg_tb_sigtab,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_tb_sigtab'},
            dag=dag)
        
        stg_leitos = PythonOperator(
            task_id='stg_leitos',
            python_callable=run_stg_leitos,
            op_kwargs={
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_leitos'},
            dag=dag)

        stg_sim = PythonOperator(
            task_id='stg_sim',
            python_callable=run_sim,
            op_kwargs={
                'ufs': ufs,
                'start_year': start_year,
                'end_year': end_year,
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_sim'},
            dag=dag)

        stg_sih = PythonOperator(
            task_id='stg_sih',
            python_callable=run_sih,
            op_kwargs={
                'ufs': ufs,
                'start_year': start_year,
                'end_year': end_year,
                'con': con,
                'schema': stg_schema,
                'tb_name': 'stg_sih'},
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

        d_sexo = PythonOperator(
            task_id='d_sexo',
            python_callable=run_d_sexo,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_sexo'},
            dag=dag)

        d_estabelecimento = PythonOperator(
            task_id='d_estabelecimento',
            python_callable=run_d_estabelecimento,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_estabelecimento'},
            dag=dag)

        d_procedimento = PythonOperator(
            task_id='d_procedimento',
            python_callable=run_d_procedimento,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_procedimento'},
            dag=dag)

        d_especialidade = PythonOperator(
            task_id='d_especialidade',
            python_callable=run_d_especialidade,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'd_especialidade'},
            dag=dag)


    with TaskGroup(group_id='fatos') as fatos:
        f_obito = PythonOperator(
            task_id='f_obito',
            python_callable=run_f_obito,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'f_obito',
                'start_year': start_year,
                'end_year': end_year},
            dag=dag)
        
        f_hospitalizacao = PythonOperator(
            task_id='f_hospitalizacao',
            python_callable=run_f_hospitalizacao,
            op_kwargs={
                'con': con,
                'schema': dw_schema,
                'tb_name': 'f_hospitalizacao'},
            dag=dag)


    struct_db >> stages >> dimensoes >> fatos
