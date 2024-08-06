from datetime import datetime, timedelta
# 스케줄 관련 모듈
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='corona_etl_v2',
    default_args={
        'depends_on_past' : False,
        'email' : ['abcmemory@gm.com'],
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries' : 2,
        'retry_delay' : timedelta(minutes=5)
    },
    description='corona etl pipline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 5, 13, 00),
    catchup = False, # 문법적 오류등으로 실행되지 않는 DAG를 다시 실행할 것인지 여부
    tags=['corona_etl_v2']
) as dag:
    t1 = BashOperator(
        task_id = 'extract_corona_api_v2',
        cwd = '/root/study/corona_project',
        bash_command='python3 corona_extract.py'
    )

    t2 = BashOperator(
        task_id = 'transDW_corona_api_v2',
        cwd = '/root/study/corona_project',
        bash_command='python3 corona_transform_dw.py'
    )

dag.doc_md = """
    This is corona project~
"""

t1 >> t2

# 순서적 실행 : t1 >> t2
# 병렬 실행 : [t1, t2]
# 복합으로 생성 가능 : [t3, t4] >> t2 >> t1 >> [t6, t7]