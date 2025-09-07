from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from plugins.operators.data_extraction import DataExtractionOperator
from plugins.operators.data_transformation import DataTransformationOperator
from plugins.operators.data_loading import DataLoadingOperator
from plugins.helpers.sql_queries import SqlQueries
from config.settings import DATABASE_CONFIG, ETL_CONFIG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def validate_data_quality(execution_date):
    """Validate data quality after processing"""
    import sqlite3
    from plugins.helpers.sql_queries import SqlQueries
    from config.settings import DATABASE_CONFIG
    
    conn = sqlite3.connect(DATABASE_CONFIG['database'])
    cursor = conn.cursor()
    
    cursor.execute(SqlQueries.data_quality_check)
    result = cursor.fetchone()
    
    print(f"Data Quality Check Results:")
    print(f"Total Records: {result[0]}")
    print(f"Unique Transactions: {result[1]}")
    print(f"Null Quantities: {result[2]}")
    print(f"Null Amounts: {result[3]}")
    
    # Basic validation rules
    if result[0] == 0:
        raise ValueError("No records processed!")
    if result[2] > 0:
        print("Warning: Some quantities are null")
    
    cursor.close()
    conn.close()

def generate_analytics(execution_date):
    """Generate analytics data"""
    import sqlite3
    from plugins.helpers.sql_queries import SqlQueries
    from config.settings import DATABASE_CONFIG
    
    conn = sqlite3.connect(DATABASE_CONFIG['database'])
    cursor = conn.cursor()
    
    try:
        cursor.execute(SqlQueries.analytics_query)
        conn.commit()
        print("Analytics generated successfully!")
    except Exception as e:
        conn.rollback()
        print(f"Error generating analytics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# Define file paths
current_date = "{{ ds }}"
raw_file_path = f"{ETL_CONFIG['raw_data_path']}/sales_data_{current_date}.csv"
processed_file_path = f"{ETL_CONFIG['processed_data_path']}/processed_sales_{current_date}.csv"

start_task = DummyOperator(task_id='start_etl', dag=dag)

extract_task = DataExtractionOperator(
    task_id='extract_sales_data',
    source_type='csv',
    source_config={
        'file_path': str(ETL_CONFIG['raw_data_path'] / 'sample_sales_data.csv')
    },
    output_path=raw_file_path,
    dag=dag,
)

transform_task = DataTransformationOperator(
    task_id='transform_sales_data',
    input_path=raw_file_path,
    output_path=processed_file_path,
    transformation_rules={
        'missing_values': {
            'quantity': 0,
            'amount': 'mean',
            'store_location': 'Unknown'
        },
        'transformations': {
            'transaction_date': {'type': 'datetime'},
            'quantity': {'type': 'numeric'},
            'amount': {'type': 'numeric'}
        }
    },
    dag=dag,
)

load_task = DataLoadingOperator(
    task_id='load_to_database',
    input_path=processed_file_path,
    table_name='processed_sales_data',
    connection_config=DATABASE_CONFIG,
    batch_size=1000,
    dag=dag,
)

generate_analytics_task = PythonOperator(
    task_id='generate_analytics',
    python_callable=generate_analytics,
    op_kwargs={'execution_date': current_date},
    dag=dag,
)

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=validate_data_quality,
    op_kwargs={'execution_date': current_date},
    dag=dag,
)

end_task = DummyOperator(task_id='end_etl', dag=dag)

# Define task dependencies
start_task >> extract_task >> transform_task >> load_task
load_task >> generate_analytics_task >> data_quality_check >> end_task
