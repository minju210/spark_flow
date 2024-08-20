import json
import requests
import pandas as pd
from pyspark.sql import SparkSession  # SparkSession 임포트 추가
from pyspark.sql.functions import explode_outer, col, size
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
    'movies_dynamic_json',
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
    #shedule_interval=
    schedule="@once",
    #schedule="10 2 * * *",
    start_date=datetime(2017, 1, 1),
    end_date=datetime(2017,1, 1),
    catchup=True,
    tags=['movie', 'dynamic', 'json', 'sql'],
) as dag:
    def save_data(ds_nodash):
        from movdata.yj import save_movies
        year = str(ds_nodash)[:4]
        
        save_movies(year=year)
    
    #ok~~~ 코딩 갈 준비 완료~~~
    
    def parse_data():
        # SparkSession 생성
        spark = SparkSession.builder.appName("parquet processing").getOrCreate()
        
        # JSON 파일 읽기
        jdf = spark.read.option("multiline","true").json('/home/minjoo/code/movdata/data/movies/year=2017/data.json')

        from pyspark.sql.functions import explode_outer, col, size
        ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))
        
        edf = ccdf.withColumn("company", explode_outer("companys"))

        eedf = edf.withColumn("director", explode_outer("directors"))
        
        eedf.select(
            col("movieCd"),
            col("company.companyCd").alias("companyCd"),
            col("company.companyNm").alias("companyNm"),
            col("director")
        ).write.mode("overwrite").parquet("/home/minjoo/output/parsed_data.parquet")

        spark.stop()

    def select_data():
        spark = SparkSession.builder.appName("select parquet").getOrCreate()

        # Parquet 파일 읽기
        eedf = spark.read.parquet("/home/minjoo/output/parsed_data.parquet")

        # 감독별로 영화 집계
        grouped_by_director = eedf.groupBy("director").count()

        # 회사별로 영화 집계
        grouped_by_company = eedf.groupBy("companyNm").count()

        # 집계 결과 저장
        grouped_by_director.write.mode("overwrite").parquet("/home/minjoo/output/director_count.parquet")
        grouped_by_company.write.mode("overwrite").parquet("/home/minjoo/output/company_count.parquet")
        
        grouped_by_director.show()
        grouped_by_company.show()

        # Spark 세션 종료
        spark.stop()

    
    get_data = PythonOperator(
        task_id='get.data',
        python_callable=save_data,
    )
    

    parsing_parquet = PythonOperator(
        task_id='parsing.parquet',
        python_callable=parse_data,
    )
    
    select_parquet = PythonOperator(
        task_id='select.parquet',
        python_callable=select_data,
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    
    # flow
    start >> get_data >> parsing_parquet >> select_parquet >> end
