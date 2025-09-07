import sqlite3
import sys
from pathlib import Path
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import DATABASE_CONFIG


def ensure_columns(cursor, table_name, required_columns):
    """
    Ensure required columns exist in the table. Add missing ones.
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_cols = [row[1] for row in cursor.fetchall()]

    for col_name, col_def in required_columns.items():
        if col_name not in existing_cols:
            print(f"[MIGRATION] Adding missing column '{col_name}' to '{table_name}'...")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")


def print_schema(cursor, table_name):
    """Prints the schema of a given table for debugging."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    print(f"\nSchema for table {table_name}:")
    for col in cursor.fetchall():
        print(f" - {col[1]} ({col[2]})")


def setup_database():
    """Create or update database tables for ETL pipeline"""

    try:
        db_path = DATABASE_CONFIG['database']

        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create raw data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT,
            product_id TEXT,
            customer_id TEXT,
            transaction_date TIMESTAMP,
            quantity INTEGER,
            amount REAL,
            store_location TEXT,
            raw_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Drop processed_sales_data table if it exists to fix schema issues
        cursor.execute("DROP TABLE IF EXISTS processed_sales_data")
        
        # Create processed data table with all required columns
        cursor.execute("""
        CREATE TABLE processed_sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE,
            product_id TEXT,
            customer_id TEXT,
            transaction_date DATE,
            quantity INTEGER,
            amount REAL,
            store_location TEXT,
            day_of_week TEXT,
            month TEXT,
            quarter INTEGER,
            year INTEGER,
            total_amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        print("[MIGRATION] Recreated processed_sales_data table with correct schema")

        # Create analytics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_date DATE,
            metric_type TEXT,
            store_location TEXT,
            total_sales REAL,
            total_transactions INTEGER,
            avg_transaction_value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        conn.commit()

        # Print schemas for debugging
        print_schema(cursor, "raw_sales_data")
        print_schema(cursor, "processed_sales_data")
        print_schema(cursor, "sales_analytics")

        cursor.close()
        conn.close()
        print("\n✅ Database tables created/updated successfully!")

    except Exception as e:
        print(f"❌ Error creating database tables: {e}")
        raise


if __name__ == "__main__":
    setup_database()
