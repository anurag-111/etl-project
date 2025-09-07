class SqlQueries:
    # Analytics query for SQLite
    analytics_query = """
    INSERT INTO sales_analytics (metric_date, metric_type, store_location, total_sales, total_transactions, avg_transaction_value)
    SELECT 
        transaction_date as metric_date,
        'daily' as metric_type,
        store_location,
        SUM(total_amount) as total_sales,
        COUNT(*) as total_transactions,
        AVG(total_amount) as avg_transaction_value
    FROM processed_sales_data
    WHERE date(transaction_date) = date('now', '-1 day')
    GROUP BY transaction_date, store_location
    """
    
    # Data quality check query
    data_quality_check = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT transaction_id) as unique_transactions,
        SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_quantities,
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
    FROM processed_sales_data
    WHERE date(transaction_date) = date('now', '-1 day');
    """
