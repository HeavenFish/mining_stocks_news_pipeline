from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')

from src.extract import fetch_price, fetch_raw_news
from src.transform import vectorize_news
from src.load import load_price, load_raw_news, load_embedded_news

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 10),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_info_pipeline',
    default_args=default_args,
    description='Daily Stock Price and News Embedding Pipeline',
    schedule_interval='@daily', # รันทุกวัน
    catchup=False
) as dag:

    # 1. Fetch & Load Price
    def price_pipeline(**context):
        execution_date = context['ds'] # ดึงวันที่จาก Airflow (YYYY-MM-DD)
        df_price = fetch_price(execution_date)
        load_price(df_price)

    task_price = PythonOperator(
        task_id='price_pipeline',
        python_callable=price_pipeline,
    )

    # 2. Fetch & Load Raw News
    def news_fetch_pipeline(**context):
        execution_date = context['ds']
        parquet_path = fetch_raw_news(execution_date)
        if parquet_path:
            load_raw_news(parquet_path)
        return execution_date

    task_fetch_news = PythonOperator(
        task_id='fetch_and_load_news',
        python_callable=news_fetch_pipeline,
    )

    # 3. Vectorize News (Transform)
    def news_vectorize_pipeline(**context):
        execution_date = context['ds']
        # ตัวอย่างการส่ง path ต่อผ่าน XComs หรือใช้ execution_date อ้างอิง
        vector_parquet_path = vectorize_news(execution_date)
        return vector_parquet_path

    task_vectorize_news = PythonOperator(
        task_id='vectorize_news_step',
        python_callable=news_vectorize_pipeline,
    )

    # 4. Load Embedded News
    def news_load_vector_pipeline(**context):
        ti = context['task_instance']
        vector_path = ti.xcom_pull(task_ids='vectorize_news_step')
        load_embedded_news(vector_path)

    task_load_vector = PythonOperator(
        task_id='load_vector_to_db',
        python_callable=news_load_vector_pipeline,
    )

    # ลำดับการทำงาน
    task_price
    task_fetch_news >> task_vectorize_news >> task_load_vector