from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import datetime


DAG_NAME = 'krasnickiy_sprav_csv_hdfs_dag'

args = {'owner': 'krasnickiy',
	'start_date': datetime.datetime(2023, 9, 20),
	'retries': 1,
	'retry_delay': datetime.timedelta(minutes=5)
	}
                
with DAG(DAG_NAME,
	tags = ['krasnickiy'],
	default_args = args,
	description = 'send data from csv to hdfs parquet',
	schedule_interval = '* */12 * * *',
	catchup = False,
	params = {'labels':{'env': 'prod', 'priority': 'high'}}
	) as dag:

    task_rtk_users_csv_hdfs = SSHOperator(task_id = 'rtk_users_csv_to_hdfs',
					ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
					command = 'source venv/bin/activate; python kurs/csv_to_hdfs_rtk_users.py'
					)
    task_company_csv_hdfs = SSHOperator(task_id = 'company_csv_to_hdfs',
					ssh_conn_id = 'krasnickiy_ssh_vm-cli2_conn',
					command = 'source venv/bin/activate; python kurs/csv_to_hdfs_company.py'
					)
task_rtk_users_csv_hdfs
task_company_csv_hdfs


