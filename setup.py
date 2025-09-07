from setuptools import setup, find_packages

setup(
    name="etl_project",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas==1.5.3",
        "numpy==1.24.3",
        "psycopg2-binary==2.9.6",
        "requests==2.28.2",
        "fastapi==0.95.0",
        "uvicorn==0.21.1",
        "sqlalchemy==1.4.47",
        "python-dotenv==1.0.0",
    ],
    python_requires=">=3.8",
)
