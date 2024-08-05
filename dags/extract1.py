# extract1.py DAGS
# coded by Sehyeon
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
    "git+https://github.com/LeeChungBae/Transform_package.git@dev/d1.0.0",
    "git+https://github.com/LeeChungBae/Load_package.git@dev/d1.0.0"
]

with DAG(
        'extract1',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

# functions
    def extract():
        pass

    def icebreak():
        from extract_package.ice_breaking import ice_breaking
        ice_breaking()


# tasks
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    extract1 = PythonVirtualenvOperator(
        task_id = 'extract1',
        python_callable = extract,
        system_site_packages = False,
        requirements = REQUIREMENTS[0],
    )

    icebreaking = PythonVirtualenvOperator(
        task_id = 'icebreaking',
        python_callable = icebreak,
        system_site_packages = False,
        requirements = REQUIREMENTS[0],
        trigger_rule = 'all_done'
    )

start >> extract1 >> icebreaking >> end
