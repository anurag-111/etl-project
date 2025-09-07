# ETL Pipeline with Airflow & FastAPI

A complete, end-to-end Extract, Transform, Load (ETL) pipeline built with Python, Apache Airflow, and FastAPI.  
This project processes sales data from raw CSV files into analytics-ready datasets, generates store-level summaries, and exposes the data through a RESTful API.

---

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
🛠️ Tech Stack
Orchestration: Apache Airflow

Processing: Python, Pandas, SQLAlchemy

Database: PostgreSQL / SQLite

API Framework: FastAPI

Containerization: Docker

Caching & Queue: Redis, Celery

📁 Project Structure
graphql
Copy
Edit
etl_project/
├── dags/
│   └── etl_pipeline.py            # Airflow DAG definition
├── plugins/
│   ├── operators/
│   │   ├── data_extraction.py     # Custom extraction operator
│   │   ├── data_transformation.py # Transformation operator
│   │   └── data_loading.py        # Database loading operator
│   └── helpers/
│       └── sql_queries.py         # SQL queries for analytics
├── api/
│   ├── main.py                    # FastAPI application
│   ├── models.py                  # SQLAlchemy models
│   └── schemas.py                 # Pydantic schemas
├── scripts/
│   ├── setup_database.py          # Database initialization
│   └── generate_sample_data.py    # Sample data generation
├── config/
│   └── settings.py                # Configuration settings
└── storage/
    ├── raw/                       # Raw data storage
    ├── processed/                 # Processed data storage
    └── analytics/                 # Analytics results
🚀 Quick Start
Prerequisites
Python 3.8+

PostgreSQL (or SQLite for development)

Docker (optional)

Installation
Clone the repository:

bash
Copy
Edit
git clone https://github.com/anurag-111/etl-project.git
cd etl-project
Create virtual environment:

bash
Copy
Edit
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Set up environment variables:

bash
Copy
Edit
cp .env.example .env
# Edit .env with your database credentials
Initialize database:

bash
Copy
Edit
python scripts/setup_database.py
Generate sample data:

bash
Copy
Edit
python scripts/generate_sample_data.py
Running the Pipeline
Option 1: Using Airflow (Recommended)

bash
Copy
Edit
# Initialize Airflow database
airflow db init

# Create user
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com

# Start services
airflow webserver --port 8080
airflow scheduler

# Trigger DAG (in new terminal)
airflow dags trigger sales_etl_pipeline
Option 2: Standalone execution

bash
Copy
Edit
python scripts/run_etl.py
Start the API
bash
Copy
Edit
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
