from stg.STG_SIM import Stg_SIM


conn_params = {
    'dbname': 'saude_mental',
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'schema': 'stg',
    'table_name': 'sim_2010_2020'}

Stg_SIM.run(
    year_start=2010,
    year_end=2020,
    uf='ES',
    info_sys='SIM-DO',
    conn_params=conn_params
)
