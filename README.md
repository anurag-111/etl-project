# ETL Pipeline with Airflow & FastAPI

A complete, end-to-end Extract, Transform, Load (ETL) pipeline built with Python, Apache Airflow, and FastAPI. This project processes sales data from raw CSV files into analytics-ready datasets, generates store-level summaries, and exposes the data through a RESTful API.

## 🏗️ Architecture

```mermaid
graph TD
    A[Data Sources] --> B[Airflow Orchestration];
    B --> C[Extraction];
    C --> D[Raw Data Storage];
    D --> E[Transformation];
    E --> F[Processed Data Storage];
    F --> G[Loading];
    G --> H[Data Warehouse];
    H --> I[Analytics & API];
    
    subgraph "Monitoring"
        J[Airflow UI]
        K[Data Quality Checks]
        L[Logging]
    end
    
    B --> J
    E --> K
    G --> L
