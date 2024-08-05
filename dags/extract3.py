# extract3.py DAGS

from datetime import datetime, timedelta
from textwrap import dedent

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

REQUIREMENTS = "git+https://github.com/LeeChungBae/Extract_package.git@dev/d2.0.0"

#REQUIREMENTS = [
#    "git+https://github.com/LeeChungBae/Extract_package.git@dev/d2.0.0",
#    "git+https://github.com/LeeChungBae/Load_package.git@dev/d2.0.0-sang",
#    "git+https://github.com/LeeChungBae/Transform_package.git@dev/d2.0.0-sang",
#]
    

with DAG(
        'extract3',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    schedule="10 2 * * *",
    start_date=datetime(2023, 9, 1),
    end_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    # func
    def extract(**kwargs):
        from extract_package.extract3 import save2df
        dt = kwargs['ds_nodash']
        parq_path = kwargs['PARQ_PATH']

        df = save2df(dt)
        print(df.head(5))
    
        df.to_parquet(parq_path , partition_cols=['load_dt'] )


    def icebreak():
        from  extract_package.ice_breaking import ice_breaking
        ice_breaking()
    
    # task
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    rm_dir = BashOperator(
        task_id='rm_dir',
        bash_command='''
            echo "{{ var.value.TP_PATH }}/extract_path"
            EXTR_PATH={{ var.value.TP_PATH }}/extract_path
            rm -rf $EXTR_PATH/load_dt={{ds_nodash}}
        '''
    )

    extract3 = PythonVirtualenvOperator(
        task_id = 'extract3',
        python_callable=extract,
        op_kwargs = { 'PARQ_PATH': "{{var.value.TP_PATH}}/extract_path" },
        requirements=REQUIREMENTS,
        system_site_packages=False,
    )

    icebreaking = PythonVirtualenvOperator(
         task_id = 'icebreaking',
         python_callable=icebreak,
         requirements=REQUIREMENTS[0],
         system_site_packages=False,
    )

    start >> rm_dir >>  extract3 >> icebreaking >> end
