from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Import modules directly from the scripts directory
sys.path.insert(0, '/opt/airflow/scripts')
from scraper import RealEstateScraper
from transformer import RealEstateTransformer
from loader import RealEstateLoader

def scrape_properties(ti):
    """Scrape properties from DealApp"""
    scraper = RealEstateScraper()
    properties = scraper.scrape_dealapp(max_pages=20, district=None)
    df_scraped = pd.DataFrame(properties)
    ti.xcom_push(key='scraped_df', value=df_scraped.to_json(orient='records'))
    print(f"Scraped {len(df_scraped)} properties")
    return len(df_scraped)

def clean_scraped_data(ti):
    """Clean and validate scraped data"""
    df_scraped = pd.read_json(ti.xcom_pull(key='scraped_df', task_ids='scrape'), orient='records')
    transformer = RealEstateTransformer()
    df_clean = transformer.transform_scraped_data(df_scraped)
    ti.xcom_push(key='clean_df', value=df_clean.to_json(orient='records'))
    print(f"Cleaned {len(df_clean)} properties")
    return len(df_clean)

def validate_data(ti):
    """Validate data quality"""
    df_clean = pd.read_json(ti.xcom_pull(key='clean_df', task_ids='clean_scraped_data'), orient='records')
    
    # Simple validation - check for required columns and non-null values
    required_columns = ['property_id', 'price']  # Removed 'title'
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for null values in critical columns
    null_counts = df_clean[required_columns].isnull().sum()
    validation_results = {
        'total_records': len(df_clean),
        'null_counts': null_counts.to_dict(),
        'missing_columns': missing_columns
    }
    
    ti.xcom_push(key='validation_results', value=validation_results)
    print(f"Validation completed: {validation_results}")
    return validation_results

def load_data(ti):
    """Load data into database"""
    df_clean = pd.read_json(ti.xcom_pull(key='clean_df', task_ids='clean_scraped_data'), orient='records')
    loader = RealEstateLoader()
    if loader.connect_to_database():
        # Create tables only if they don't exist (preserves existing data)
        loader.create_tables_if_not_exist()
        
        # Verify unique constraint is working
        loader.verify_unique_constraint()
        
        success = loader.load_properties_dataframe_batch(df_clean)  # Use batch UPSERT
        loader.close_connection()
        if success:
            print(f"Loaded {len(df_clean)} properties into database")
            return len(df_clean)
        else:
            raise Exception("Failed to load properties into database")
    else:
        raise Exception("Failed to connect to database")

def run_analysis(ti):
    """Run all analysis calculations to populate analysis tables"""
    loader = RealEstateLoader()
    if loader.connect_to_database():
        loader.calculate_rent_increase_analysis()
        # loader.calculate_duration_analysis()  # Removed, no closed_date data
        loader.calculate_correlation_analysis()
        loader.calculate_sales_valuation()
        loader.populate_avg_price_tables()
        loader.populate_yasameen_3br_report()
        loader.close_connection()
        print("Analysis tables populated.")
    else:
        raise Exception("Failed to connect to database for analysis")

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
    'realestate_etl_pipeline_v2',
    default_args=default_args,
    description='Real estate ETL pipeline for scraped data only',
    schedule='0 0 * * 6',  # Weekly at midnight on Saturday
    catchup=False,
    tags=['realestate', 'etl', 'scraping'],
)

scrape = PythonOperator(
    task_id='scrape',
    python_callable=scrape_properties,
    dag=dag
)

clean_scraped_data = PythonOperator(
    task_id='clean_scraped_data',
    python_callable=clean_scraped_data,
    dag=dag
)

validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

analysis = PythonOperator(
    task_id='run_analysis',
    python_callable=run_analysis,
    dag=dag
)

# Update task dependencies
scrape >> clean_scraped_data >> validate_data >> load >> analysis 