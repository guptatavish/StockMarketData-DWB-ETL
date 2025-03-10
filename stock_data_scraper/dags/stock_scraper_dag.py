from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dotenv import load_dotenv
from scrapers.stock_data_scraper import StockDataScraper  # Fixed import path

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'stock_data_scraper',
    default_args=default_args,
    description='Scrape stock data and load it into BigQuery',
    schedule_interval='@daily',
)

def run_complete_pipeline():
    project_id = os.environ.get("PROJECT_ID", "marine-lodge-453310-g5")
    dataset_id = os.environ.get('DATASET_ID', "StockMktData")
    table_id = os.environ.get('TABLE_ID', "StockData")
    
    scraper = StockDataScraper(project_id, dataset_id, table_id)
    success = scraper.run_pipeline()
    if not success:
        raise Exception("Stock data ETL pipeline failed")
    return "Pipeline completed successfully"

with dag:
    pipeline_task = PythonOperator(
        task_id='run_complete_pipeline',
        python_callable=run_complete_pipeline,
    )