import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import ETL_CONFIG

def generate_sample_data(num_records=1000):
    """Generate sample sales data for testing"""
    
    # Create storage directories if they don't exist
    os.makedirs(ETL_CONFIG['raw_data_path'], exist_ok=True)
    os.makedirs(ETL_CONFIG['processed_data_path'], exist_ok=True)
    
    # Generate sample data
    np.random.seed(42)
    
    dates = [datetime.now() - timedelta(days=x) for x in range(30)]
    products = [f"PROD_{i:03d}" for i in range(1, 51)]
    customers = [f"CUST_{i:05d}" for i in range(1, 1001)]
    stores = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    
    data = []
    for i in range(num_records):
        record = {
            'transaction_id': f"TXN_{i:06d}",
            'product_id': np.random.choice(products),
            'customer_id': np.random.choice(customers),
            'transaction_date': np.random.choice(dates),
            'quantity': np.random.randint(1, 10),
            'amount': round(np.random.uniform(10, 500), 2),
            'store_location': np.random.choice(stores)
        }
        data.append(record)
    
    # Create DataFrame and save
    df = pd.DataFrame(data)
    output_path = ETL_CONFIG['raw_data_path'] / "sample_sales_data.csv"
    df.to_csv(output_path, index=False)
    
    print(f"Generated {num_records} sample records at {output_path}")
    return output_path

if __name__ == "__main__":
    generate_sample_data(1000)
