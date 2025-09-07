import os
from pathlib import Path

# Get the project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Database configurations - Using SQLite
SQLITE_CONFIG = {
    'database': str(BASE_DIR / 'etl_database.db'),
    'check_same_thread': False  # For SQLite threading
}

# Use SQLite instead of PostgreSQL
DATABASE_CONFIG = SQLITE_CONFIG

# API configurations
API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', '8000')),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true'
}

# ETL configurations
ETL_CONFIG = {
    'raw_data_path': BASE_DIR / 'storage' / 'raw',
    'processed_data_path': BASE_DIR / 'storage' / 'processed',
    'analytics_data_path': BASE_DIR / 'storage' / 'analytics',
    'batch_size': 1000
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': BASE_DIR / 'logs' / 'etl_pipeline.log',
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'standard'
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    'loggers': {
        '': {
            'handlers': ['file', 'console'],
            'level': 'INFO',
            'propagate': True
        },
    }
}
