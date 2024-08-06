from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'lab04',
    'start_date': datetime(2022, 2, 1),
    'retries': 1,
}

dag = DAG(
    'csv_to_hdfs_bash',
    default_args=default_args,
    description='DAG to upload CSV to HDFS using BashOperator',
    schedule_interval=timedelta(days=1),
)

# Specify the local and HDFS paths
local_csv_path = '/path/to/local/file.csv'
hdfs_upload_path = '/path/in/hdfs/file.csv'

# Bash command to upload CSV to HDFS using hdfs dfs -copyFromLocal command
upload_command = f"hdfs dfs -copyFromLocal {local_csv_path} {hdfs_upload_path}"

# BashOperator to execute the upload command
upload_task = BashOperator(
    task_id='upload_csv_to_hdfs',
    bash_command=upload_command,
    dag=dag,
)

# You can add more tasks or dependencies as needed
# For example, you might want to perform additional processing after the upload.

upload_task