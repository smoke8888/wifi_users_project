from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import datetime


DAG_NAME = 'krasnickiy_data_csv_hdfs_dag'

args = {'owner': 'krasnickiy',
	'start_date': datetime.datetime(2023, 9, 20),
	'retries': 1,
	'retry_delay': datetime.timedelta(minutes=5)
	}
                
with DAG(DAG_NAME,
	tags = ['krasnickiy'],
	default_args = args,
	description = 'send data from csv to hdfs parquet',
	schedule_interval = '*/25 * * * *',
	catchup = False,
	params = {'labels':{'env': 'prod', 'priority': 'high'}}
	) as dag:

    task_wifi_users_generate = SSHOperator(task_id = 'wifi_users_generate',
                                        ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
                                        command = 'source venv/bin/activate; python3 kurs/generate_wifi_users.py'
                                        )

    task_wifi_users_csv_hdfs = SSHOperator(task_id = 'wifi_users_csv_to_hdfs',
					ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
					command = 'source venv/bin/activate; python3 kurs/csv_to_hdfs_wifi_users.py'
					)
task_wifi_users_generate >> task_wifi_users_csv_hdfs


