from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional

class SalesData(BaseModel):
    transaction_id: str
    product_id: str
    customer_id: str
    transaction_date: date
    quantity: int
    amount: float
    store_location: str
    day_of_week: Optional[str] = None
    month: Optional[str] = None
    quarter: Optional[str] = None
    total_amount: float
    
    class Config:
        orm_mode = True

class SalesAnalytics(BaseModel):
    metric_date: date
    metric_type: str
    store_location: str
    total_sales: float
    total_transactions: int
    avg_transaction_value: float
    
    class Config:
        orm_mode = True
