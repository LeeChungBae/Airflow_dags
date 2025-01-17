# transform1.py
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

REQUIREMENTS = "git+https://github.com/LeeChungBae/Transform_package.git@dev/d2.0.0-say"
#[
#    "git+https://github.com/LeeChungBae/Extract_package.git@dev/d2.0.0",
#    "git+https://github.com/LeeChungBae/Transform_package.git@dev/d2.0.0-say",
#    "git+https://github.com/LeeChungBae/Load_package.git@dev/d1.0.0"
#]

with DAG(
        'transform3',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    schedule="10 2 * * *",
    start_date=datetime(2023, 9, 1),
    end_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    # functions
    def transform1(**kwargs):
        from transform_package.transform import col_drop, str_to_num
        ds_nodash = kwargs['ds_nodash']
        extr_path = kwargs['extr_path']
        trans_path = kwargs['trans_path'] 
        load_dt = kwargs['ds_nodash']
     
        df = col_drop(ds_nodash, extr_path)
        num_df = str_to_num(df=df, path=trans_path, load_dt=load_dt)

    # tasks
    # wrapper EmptyOperators
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    # idempotency Bash Operators
    # uses airflow variable <TP_PATH>
    trans_path = BashOperator(
        task_id = "trans_path",
        bash_command='''
            TRANS_PATH={{ var.value.TP_PATH }}/transform_path
            echo "checking TRANS_PATH $TRANS_PATH..."
            if [[ -d "$TRANS_PATH/load_dt={{ds_nodash}}" ]]; then
                echo "rm -rf $TRANS_PATH"
                rm -rf $TRANS_PATH
                echo "[INFO] Removed preexisting path and files..."
            else
                echo "[INFO] No preexisting path and files found. Continuing..."
            fi

            echo "[CMD] mkdir -p $TRANS_PATH"
            mkdir -p $TRANS_PATH
        '''
    )

    # main tasks
    transform1 = PythonVirtualenvOperator(
        task_id = 'transform1',
        python_callable = transform1,
        op_kwargs = {
            'extr_path' : "{{var.value.TP_PATH}}/extract_path", 
            'trans_path' : "{{var.value.TP_PATH}}/transform_path" 
        },
        system_site_packages = False,
        requirements = REQUIREMENTS,
        trigger_rule = "all_success"
    )

start >> trans_path >> transform1 >> end
