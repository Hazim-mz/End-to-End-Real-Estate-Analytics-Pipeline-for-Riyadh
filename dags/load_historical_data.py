from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Import modules directly from the scripts directory
sys.path.insert(0, '/opt/airflow/scripts')
from transformer import RealEstateTransformer
from loader import RealEstateLoader

def load_historical_data_task():
    """Load historical data only once"""
    transformer = RealEstateTransformer()
    loader = RealEstateLoader()
    
    # Load and transform historical data
    df_historical = transformer.run_merge_and_store_warehouse()
    
    if df_historical.empty:
        print("No historical data available")
        return 0
    
    # Load into database
    if loader.connect_to_database():
        # Create tables only if they don't exist (preserves existing data)
        loader.create_tables_if_not_exist()
        success = loader.load_properties_dataframe_batch(df_historical)  # Use batch UPSERT
        loader.close_connection()
        if success:
            print(f"Loaded {len(df_historical)} historical records")
            return len(df_historical)
        else:
            raise Exception("Failed to load historical data into database")
    else:
        raise Exception("Failed to connect to database")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_historical_data',
    default_args=default_args,
    description='Load historical real estate data once',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['realestate', 'historical', 'one-time'],
)

load_historical = PythonOperator(
    task_id='load_historical',
    python_callable=load_historical_data_task,
    dag=dag
) 