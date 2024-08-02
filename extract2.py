# extract2.py DAGS

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
        'extract2',
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
    
    def ex2():
        from extract_package.ice_breaking import ice_breaking
        ice_breaking()
        
        

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    extract2 = PythonVirtualenvOperator(
        task_id = 'extract2',
        python_callable = ex2,
        system_site_packages=False,
        requirements=REQUIREMENTS[0],
)

start >> extract2 >> end
