from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from . import models, schemas
from .database import SessionLocal, engine
from config.settings import API_CONFIG

# Create database tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="ETL Data API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "ETL Data API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/sales/data", response_model=List[schemas.SalesData])
async def get_sales_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    store_location: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get sales data with filters"""
    try:
        query = db.query(models.SalesData)
        
        if start_date:
            query = query.filter(models.SalesData.transaction_date >= start_date)
        if end_date:
            query = query.filter(models.SalesData.transaction_date <= end_date)
        if store_location:
            query = query.filter(models.SalesData.store_location == store_location)
        
        return query.limit(limit).all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sales/analytics/daily")
async def get_daily_analytics(
    date: Optional[datetime] = None,
    db: Session = Depends(get_db)
):
    """Get daily sales analytics"""
    try:
        target_date = date or datetime.now().date()
        
        result = db.query(models.SalesAnalytics).filter(
            models.SalesAnalytics.metric_date == target_date,
            models.SalesAnalytics.metric_type == 'daily'
        ).all()
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=API_CONFIG['host'],
        port=API_CONFIG['port'],
        reload=API_CONFIG['debug']
    )
