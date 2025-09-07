from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, Any

class DataTransformationOperator(BaseOperator):
    """
    Operator to transform extracted data
    """
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        output_path: str,
        transformation_rules: Dict[str, Any],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.output_path = output_path
        self.transformation_rules = transformation_rules
    
    def execute(self, context):
        self.log.info("Starting data transformation")
        
        try:
            # Load data
            data = pd.read_csv(self.input_path)
            
            # Apply transformations
            data = self._clean_data(data)
            data = self._transform_data(data)
            data = self._enrich_data(data)
            
            # Save transformed data
            data.to_csv(self.output_path, index=False)
            self.log.info(f"Transformed data saved to {self.output_path}")
            
            return self.output_path
            
        except Exception as e:
            self.log.error(f"Transformation failed: {e}")
            raise
    
    def _clean_data(self, data):
        """Clean the data"""
        # Remove duplicates
        data = data.drop_duplicates()
        
        # Handle missing values
        for column, strategy in self.transformation_rules.get('missing_values', {}).items():
            if column in data.columns:
                if strategy == 'mean':
                    data[column] = data[column].fillna(data[column].mean())
                elif strategy == 'median':
                    data[column] = data[column].fillna(data[column].median())
                elif strategy == 'drop':
                    data = data.dropna(subset=[column])
                elif isinstance(strategy, (int, float, str)):
                    data[column] = data[column].fillna(strategy)
        
        return data
    
    def _transform_data(self, data):
        """Apply transformations to data"""
        transformations = self.transformation_rules.get('transformations', {})
        
        for column, transform in transformations.items():
            if column in data.columns:
                if transform['type'] == 'datetime':
                    data[column] = pd.to_datetime(data[column])
                elif transform['type'] == 'categorical':
                    data[column] = data[column].astype('category')
                elif transform['type'] == 'numeric':
                    data[column] = pd.to_numeric(data[column], errors='coerce')
        
        return data
    
    def _enrich_data(self, data):
        """Add derived columns and enrich data"""
        # Add date-related features
        if 'transaction_date' in data.columns:
            data['transaction_date'] = pd.to_datetime(data['transaction_date'])
            data['day_of_week'] = data['transaction_date'].dt.day_name()
            data['month'] = data['transaction_date'].dt.month_name()
            data['quarter'] = data['transaction_date'].dt.quarter
            data['year'] = data['transaction_date'].dt.year
        
        # Add calculated fields
        if all(col in data.columns for col in ['quantity', 'amount']):
            data['total_amount'] = data['quantity'] * data['amount']
        
        return data
