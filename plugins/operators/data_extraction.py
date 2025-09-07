from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import requests
from typing import Dict, Any
import json
import logging

class DataExtractionOperator(BaseOperator):
    """
    Operator to extract data from various sources
    """
    
    @apply_defaults
    def __init__(
        self,
        source_type: str,
        source_config: Dict[str, Any],
        output_path: str,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_type = source_type
        self.source_config = source_config
        self.output_path = output_path
    
    def execute(self, context):
        self.log.info(f"Extracting data from {self.source_type}")
        
        try:
            if self.source_type == 'api':
                data = self._extract_from_api()
            elif self.source_type == 'csv':
                data = self._extract_from_csv()
            elif self.source_type == 'database':
                data = self._extract_from_database()
            else:
                raise ValueError(f"Unsupported source type: {self.source_type}")
            
            # Save extracted data
            data.to_csv(self.output_path, index=False)
            self.log.info(f"Data extracted and saved to {self.output_path}")
            
            return self.output_path
            
        except Exception as e:
            self.log.error(f"Extraction failed: {e}")
            raise
    
    def _extract_from_api(self):
        """Extract data from API"""
        response = requests.get(
            self.source_config['url'],
            params=self.source_config.get('params', {}),
            headers=self.source_config.get('headers', {}),
            timeout=30
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())
    
    def _extract_from_csv(self):
        """Extract data from CSV file"""
        return pd.read_csv(self.source_config['file_path'])
    
    def _extract_from_database(self):
        """Extract data from database"""
        import psycopg2
        conn = psycopg2.connect(**self.source_config['connection'])
        query = self.source_config['query']
        return pd.read_sql(query, conn)
