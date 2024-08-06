from datetime import datetime, timedelta

# DAG 객체 모듈
from airflow import DAG

# 스케줄 작업 동작 함수(클래스)
from airflow.operators.bash import BashOperator

d_arg = {
    'owner' : 'admin',
}
# 스케줄 관리 객체(스케줄 1개)
corona_dag = DAG(
    dag_id = 'corona_extract',
    default_args = d_arg,
    start_date = datetime(2024, 1, 3),
    dagrun_timeout = timedelta(minutes=5),
    schedule_interval = timedelta(day=1)
)

# 스케줄 작업
extractT1 = BashOperator(
    task_id = 'Bash_test',
    bash_command="sh /root/study/corona_project/corona_extract.sh",
    dag = corona_dag
)