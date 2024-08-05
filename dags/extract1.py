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
    "git+https://github.com/LeeChungBae/Extract_package.git@dev/d2.0.0-say",
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
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 1, 5),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

# functions
    def extract1(**kwargs):
        from extract_package.extract1 import save_pq
        ds_nodash = kwargs['ds_nodash']
        parq_path = kwargs['parq_path']
        df = save_pq(ds_nodash, parq_path)
        print(df.head(5))

    def icebreak():
        from extract_package.ice_breaking import ice_breaking
        ice_breaking()


# tasks

    # wrapper EmptyOperators
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    # idempotency Bash Operators

    # extract_path location stored in airflow variable <EXTRACT_PATH>
    parq_path = BashOperator(
        task_id = "parq_path",
        bash_command='''
            EXTR_PATH={{ var.value.EXTRACT_PATH }}
            echo "checking EXTRACT_PATH $EXTR_PATH..."
            if [[ -d "$EXTR_PATH/load_dt={{ds_nodash}}" ]]; then
                echo "rm -rf $EXT_PATH"
                rm -rf $EXTR_PATH
                echo "[INFO] Removed preexisting path and files..."
            else
                echo "[INFO] No preexisting path and files found. Continuing..."
            fi
            
            echo "[CMD] mkdir -p $EXTR_PATH"
            mkdir -p $EXTR_PATH
        '''
    )

    # main tasks
    extract1 = PythonVirtualenvOperator(
        task_id = 'extract1',
        python_callable = extract1,
        op_kwargs = { 'parq_path' : "{{var.value.EXTRACT_PATH}}" },
        system_site_packages = False,
        requirements = REQUIREMENTS[0],
        trigger_rule = "all_success"
    )

    icebreaking = PythonVirtualenvOperator(
        task_id = 'icebreaking',
        python_callable = icebreak,
        system_site_packages = False,
        requirements = REQUIREMENTS[0],
        trigger_rule = 'all_done'
    )

start >> parq_path >> extract1 >> icebreaking >> end