from sqlalchemy import Column, Integer, String, DateTime, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SalesData(Base):
    __tablename__ = "processed_sales_data"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    product_id = Column(String, index=True)
    customer_id = Column(String, index=True)
    transaction_date = Column(Date, index=True)
    quantity = Column(Integer)
    amount = Column(Numeric(10, 2))
    store_location = Column(String)
    day_of_week = Column(String)
    month = Column(String)
    quarter = Column(String)
    total_amount = Column(Numeric(12, 2))
    created_at = Column(DateTime)

class SalesAnalytics(Base):
    __tablename__ = "sales_analytics"
    
    id = Column(Integer, primary_key=True, index=True)
    metric_date = Column(Date, index=True)
    metric_type = Column(String)
    store_location = Column(String, index=True)
    total_sales = Column(Numeric(15, 2))
    total_transactions = Column(Integer)
    avg_transaction_value = Column(Numeric(10, 2))
    created_at = Column(DateTime)
