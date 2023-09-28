from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime


DAG_NAME = 'krasnickiy_wifi_kurs_dag'

SQL_STAT = 'REFRESH MATERIALIZED VIEW mv_krasnickiy_wifi_data;'

args = {'owner': 'krasnickiy',
	'start_date': datetime.datetime(2023, 9, 20),
	'retries': 1,
	'retry_delay': datetime.timedelta(minutes=5)
	}
                
with DAG(DAG_NAME,
	tags = ['krasnickiy'],
	default_args = args,
	description = 'transport layer for data generate, loading and save to hdfs',
	schedule_interval = '*/15 * * * *',
	catchup = False,
	params = {'labels':{'env': 'prod', 'priority': 'high'}}
	) as dag:
    #ssh_hook = SSHHook(ssh_conn_id="krasnickiy_ssh_vm-cli2_conn", cmd_timeout=None)

    task_wifi_users_generate = SSHOperator(task_id = 'wifi_users_generate',
					cmd_timeout = 7200,
                                        ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
                                        command = 'source venv/bin/activate; python3 kurs/generate_wifi_users.py'
                                        )

    task_wifi_users_csv_hdfs = SSHOperator(task_id = 'wifi_users_csv_to_hdfs',
					ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
					command = 'source venv/bin/activate; python3 kurs/csv_to_hdfs_wifi_users.py'
					)
   
    task_rtk_users_csv_hdfs = SSHOperator(task_id = 'rtk_users_csv_to_hdfs',
                                        ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
                                        command = 'source venv/bin/activate; python kurs/csv_to_hdfs_rtk_users.py'
                                       )
    
    task_company_csv_hdfs = SSHOperator(task_id = 'company_csv_to_hdfs',
                                        ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
                                        command = 'source venv/bin/activate; python kurs/csv_to_hdfs_company.py'
                                        )

    sql_stat = PostgresOperator(task_id = "matview_update",
                                sql=SQL_STAT,
                                postgres_conn_id = 'krasnickiy_conn',
                                autocommit=True)


[task_rtk_users_csv_hdfs, task_company_csv_hdfs, task_wifi_users_generate >> task_wifi_users_csv_hdfs] >> sql_stat



