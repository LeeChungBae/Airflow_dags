# extract3.py DAGS

from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
    is_venv_installed,
)

REQUIREMENTS = [
    "git+https://github.com/LeeChungBae/Extract_package.git@dev/d1.0.0",
    "git+https://github.com/LeeChungBae/Load_package.git@dev/d1.0.0",
    "git+https://github.com/LeeChungBae/Transform_package.git@dev/d1.0.0",
]
    

with DAG(
        'extract3',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 1),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

# func
    def extract():
        pass

    def icebreak():
        from  extract_package.ice_breaking import ice_breaking
        ice_breaking()

# tasks
    extract3 = PythonVirtualenvOperator(
         task_id = 'extract3',
         python_callable=extract,
         requirements=REQUIREMENTS[0],
         system_site_packages=False,
    )

    icebreaking = PythonVirtualenvOperator(
         task_id = 'icebreaking',
         python_callable=icebreak,
         requirements=REQUIREMENTS[0],
         system_site_packages=False,
    )

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    start >> extract3 >> icebreaking >> end
