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
        "git+https://github.com/LeeChungBae/Extract_package.git@dev/d2.0.0-joo",
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
    start_date=datetime(2023, 5, 1),
    end_date=datetime(2023, 5, 3),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:
    
    def extract2(**kwargs):
        from extract_package.extract2 import save_parquet
        ds_nodash = kwargs['ds_nodash']
        parq_path = kwargs['parq_path']
        print(parq_path)
        df = save_parquet(ds_nodash, parq_path)
        print(df.head(5))

    def icebreak():
        from extract_package.ice_breaking import ice_breaking
        ice_breaking()    

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    parq_path = BashOperator(
        task_id = "parq_path",
        bash_command='''
            EXTR_PATH={{ var.value.TP_PATH }}/extract_path
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

    extract2 = PythonVirtualenvOperator(
        task_id = 'extract2',
        python_callable = extract2,
        op_kwargs={'parq_path': "{{var.value.TP_PATH}}/extract_path"},
        system_site_packages=False,
        requirements=REQUIREMENTS[0],
        trigger_rule = "all_success"
    )

    icebreaking = PythonVirtualenvOperator(
        task_id = 'icebreaking',
        python_callable = icebreak,
        system_site_packages = False,
        requirements = REQUIREMENTS[0],
        trigger_rule = 'all_done'    
    )

    start  >> parq_path >> extract2 >> icebreaking >> end
