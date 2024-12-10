from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )

# import func
#from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df

with DAG(
    'pyspark_movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    #max_active_tasks=3,
    description='processing pyspark for movie data',
    #schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 3),
    catchup=True,
    tags=['movie', 'api', 'amt', 'pyspark'],
) as dag:
    def re_partition(ds_nodash):
        from spark_flow.re import re_part
        re_part(ds_nodash)
#    re_task = PythonVirtualenvOperator(
#        task_id='re.partition',
#        python_callable=re_partition,
#        requirements=["git+https://github.com/minju210/spark_flow.git@0.2.0/airflowdag"],
#        system_site_packages=False,
#    )
    re_task = PythonOperator(
        task_id='re.partition',
        python_callable=re_partition,
    )
    

    join_df = BashOperator(
        task_id='join.df',
        bash_command='''
            echo "{{ds_nodash}}"
            '''
    )
    
    agg_df = BashOperator(
        task_id='agg.df',
        bash_command='''
            echo "{{ds_nodash}}"
            '''
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    
    # flow
    start >> re_task >> join_df >> agg_df >> end
