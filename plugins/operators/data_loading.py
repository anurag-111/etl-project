from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import sqlite3
from typing import Dict, Any

class DataLoadingOperator(BaseOperator):
    """
    Operator to load transformed data to destination (SQLite)
    """
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        table_name: str,
        connection_config: Dict[str, Any],
        batch_size: int = 1000,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.table_name = table_name
        self.connection_config = connection_config
        self.batch_size = batch_size
    
    def execute(self, context):
        self.log.info(f"Loading data to {self.table_name}")
        
        try:
            # Load transformed data
            data = pd.read_csv(self.input_path)
            
            # Load to database
            self._load_to_sqlite(data)
            
            self.log.info("Data loading completed successfully")
            return True
            
        except Exception as e:
            self.log.error(f"Loading failed: {e}")
            raise
    
    def _load_to_sqlite(self, data):
        """Load data to SQLite database"""
        conn = sqlite3.connect(self.connection_config['database'])
        cursor = conn.cursor()
        
        # Get column names
        columns = list(data.columns)
        placeholders = ', '.join(['?' for _ in columns])
        
        # Prepare SQL query
        insert_query = f"""
            INSERT INTO {self.table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (transaction_id) DO UPDATE SET
                quantity = excluded.quantity,
                amount = excluded.amount,
                total_amount = excluded.total_amount
        """
        
        # Load data in batches
        for i in range(0, len(data), self.batch_size):
            batch = data.iloc[i:i + self.batch_size]
            records = [tuple(x) for x in batch.to_numpy()]
            
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                self.log.info(f"Loaded batch {i//self.batch_size + 1}")
            except Exception as e:
                conn.rollback()
                self.log.error(f"Error loading batch: {e}")
                raise
        
        cursor.close()
        conn.close()
