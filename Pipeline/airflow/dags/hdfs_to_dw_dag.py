
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hdfs_to_dw_bash',
    default_args=default_args,
    description='BashOperator를 사용하여 CSV를 HDFS에서 DW로 전송하는 DAG',
    schedule_interval=timedelta(days=1),
)

# BashOperator를 사용하여 Bash 스크립트 실행
hdfs_to_dw_task = BashOperator(
    task_id='hdfs_to_dw',
    bash_command='bash /path/to/hdfs_to_dw.sh',
    dag=dag,
)

# 필요에 따라 더 많은 작업이나 의존성을 추가할 수 있습니다.
# 예를 들어 전송 후 추가 처리를 수행할 수 있습니다.

hdfs_to_dw_task