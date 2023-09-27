import datetime
import requests
from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator


DAG_NAME = 'krasnickiy_MV_upd_dag'
GP_CONN_ID = 'krasnickiy_conn'

SQL_STAT = 'REFRESH MATERIALIZED VIEW mv_krasnickiy_wifi_data;'

args = {'owner': 'krasnickiy',
        'start_date': datetime.datetime(2023,9,20)
        }

with DAG(DAG_NAME,
        tags=['krasnickiy'],
        description='matview update dag',
        schedule_interval='*/30 * * * *',
        catchup=False,
        max_active_runs=1,
        default_args = args,
        params={'labels':{'env': 'prod', 'priority': 'high'}}) as dag:


    sql_stat = PostgresOperator(task_id = "matview_update",
                                sql=SQL_STAT,
                                postgres_conn_id = GP_CONN_ID,
                                autocommit=True)

sql_stat
