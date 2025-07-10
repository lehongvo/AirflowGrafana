from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import logging

# Default arguments cho DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Tạo DAG
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline cho E-commerce data',
    schedule_interval=timedelta(minutes=1),  # Chạy mỗi 1 phút thay vì 5 giây
    catchup=False,
    tags=['etl', 'ecommerce', 'data_warehouse'],
)

def extract_and_transform_customers():
    """Extract customers từ source và transform"""
    logging.info("Bắt đầu extract customers data...")
    
    try:
        # Kết nối tới database
        postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
        
        # Truncate table trước
        postgres_hook.run("TRUNCATE TABLE target_data.dim_customers CASCADE;")
        
        # Extract query
        extract_query = """
        SELECT 
            customer_id,
            CONCAT(first_name, ' ', last_name) as full_name,
            email,
            city,
            country,
            created_at::date as created_date,
            CASE 
                WHEN (SELECT COUNT(*) FROM source_data.orders o WHERE o.customer_id = c.customer_id) >= 5 THEN 'VIP'
                WHEN (SELECT COUNT(*) FROM source_data.orders o WHERE o.customer_id = c.customer_id) >= 2 THEN 'Regular'
                ELSE 'New'
            END as customer_segment
        FROM source_data.customers c
        """
        
        df = postgres_hook.get_pandas_df(extract_query)
        logging.info(f"Extracted {len(df)} customers")
        
        if len(df) > 0:
            # Transform: Thêm is_active flag
            df['is_active'] = True
            
            # Insert data với auto-generated keys
            insert_query = """
            INSERT INTO target_data.dim_customers 
            (customer_id, full_name, email, city, country, created_date, customer_segment, is_active)
            VALUES %s
            """
            
            # Chuyển đổi DataFrame thành list of tuples
            data_tuples = [tuple(row) for row in df.values]
            
            # Use psycopg2's execute_values for bulk insert
            from psycopg2.extras import execute_values
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            execute_values(cur, insert_query, data_tuples)
            conn.commit()
            cur.close()
            
            logging.info(f"Loaded {len(df)} customers vào dim_customers")
        else:
            logging.warning("Không có customer data để load")
            
    except Exception as e:
        logging.error(f"Lỗi trong extract_and_transform_customers: {str(e)}")
        raise

def extract_and_transform_products():
    """Extract products từ source và transform"""
    logging.info("Bắt đầu extract products data...")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
        
        # Truncate table trước
        postgres_hook.run("TRUNCATE TABLE target_data.dim_products CASCADE;")
        
        extract_query = """
        SELECT 
            product_id,
            product_name,
            category,
            price,
            CASE 
                WHEN price >= 500 THEN 'Premium'
                WHEN price >= 100 THEN 'Standard'
                ELSE 'Budget'
            END as price_tier,
            CASE 
                WHEN stock_quantity >= 100 THEN 'In Stock'
                WHEN stock_quantity >= 10 THEN 'Low Stock'
                ELSE 'Out of Stock'
            END as stock_status,
            supplier
        FROM source_data.products
        """
        
        df = postgres_hook.get_pandas_df(extract_query)
        logging.info(f"Extracted {len(df)} products")
        
        if len(df) > 0:
            # Insert data với auto-generated keys
            insert_query = """
            INSERT INTO target_data.dim_products 
            (product_id, product_name, category, price, price_tier, stock_status, supplier)
            VALUES %s
            """
            
            data_tuples = [tuple(row) for row in df.values]
            
            from psycopg2.extras import execute_values
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            execute_values(cur, insert_query, data_tuples)
            conn.commit()
            cur.close()
            
            logging.info(f"Loaded {len(df)} products vào dim_products")
        else:
            logging.warning("Không có product data để load")
            
    except Exception as e:
        logging.error(f"Lỗi trong extract_and_transform_products: {str(e)}")
        raise

def extract_and_transform_sales():
    """Extract sales từ source và transform vào fact table"""
    logging.info("Bắt đầu extract sales data...")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
        
        # Truncate table trước
        postgres_hook.run("TRUNCATE TABLE target_data.fact_sales CASCADE;")
        
        # Join với dimension tables để lấy đúng keys
        extract_query = """
        SELECT 
            o.order_id,
            dc.customer_key,
            dp.product_key,
            o.order_date::date as sale_date,
            oi.quantity,
            oi.unit_price,
            oi.total_price as total_amount,
            30.0 as profit_margin
        FROM source_data.orders o
        JOIN source_data.order_items oi ON o.order_id = oi.order_id
        JOIN target_data.dim_customers dc ON o.customer_id = dc.customer_id
        JOIN target_data.dim_products dp ON oi.product_id = dp.product_id
        WHERE o.status = 'completed'
        """
        
        df = postgres_hook.get_pandas_df(extract_query)
        logging.info(f"Extracted {len(df)} sales records")
        
        if len(df) > 0:
            # Insert data
            insert_query = """
            INSERT INTO target_data.fact_sales 
            (order_id, customer_key, product_key, sale_date, quantity, unit_price, total_amount, profit_margin)
            VALUES %s
            """
            
            data_tuples = [tuple(row) for row in df.values]
            
            from psycopg2.extras import execute_values
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            execute_values(cur, insert_query, data_tuples)
            conn.commit()
            cur.close()
            
            logging.info(f"Loaded {len(df)} sales records vào fact_sales")
        else:
            logging.warning("Không có sales data để load")
            
    except Exception as e:
        logging.error(f"Lỗi trong extract_and_transform_sales: {str(e)}")
        raise

def create_daily_summary():
    """Tạo daily sales summary"""
    logging.info("Tạo daily sales summary...")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
        
        # Truncate table trước
        postgres_hook.run("TRUNCATE TABLE target_data.daily_sales_summary;")
        
        summary_query = """
        INSERT INTO target_data.daily_sales_summary 
        (summary_date, total_orders, total_revenue, total_customers, avg_order_value, top_selling_category)
        SELECT 
            fs.sale_date,
            COUNT(DISTINCT fs.order_id) as total_orders,
            SUM(fs.total_amount) as total_revenue,
            COUNT(DISTINCT fs.customer_key) as total_customers,
            AVG(fs.total_amount) as avg_order_value,
            'Electronics' as top_selling_category  -- Simplified for now
        FROM target_data.fact_sales fs
        GROUP BY fs.sale_date
        """
        
        postgres_hook.run(summary_query)
        logging.info("Daily summary đã được tạo/cập nhật")
        
    except Exception as e:
        logging.error(f"Lỗi trong create_daily_summary: {str(e)}")
        raise

# Define tasks
extract_customers_task = PythonOperator(
    task_id='extract_transform_customers',
    python_callable=extract_and_transform_customers,
    dag=dag,
)

extract_products_task = PythonOperator(
    task_id='extract_transform_products',
    python_callable=extract_and_transform_products,
    dag=dag,
)

extract_sales_task = PythonOperator(
    task_id='extract_transform_sales',
    python_callable=extract_and_transform_sales,
    dag=dag,
)

create_summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag,
)

# Data quality check task
data_quality_check = PostgresOperator(
    task_id='data_quality_check',
    postgres_conn_id='postgres_data',
    sql="""
    DO $$
    DECLARE
        customer_count INTEGER;
        product_count INTEGER;
        sales_count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO customer_count FROM target_data.dim_customers;
        SELECT COUNT(*) INTO product_count FROM target_data.dim_products;
        SELECT COUNT(*) INTO sales_count FROM target_data.fact_sales;
        
        RAISE NOTICE 'Data quality check: % customers, % products, % sales', 
                     customer_count, product_count, sales_count;
                     
        IF customer_count = 0 THEN
            RAISE WARNING 'No customers in dim_customers';
        END IF;
        
        IF product_count = 0 THEN
            RAISE WARNING 'No products in dim_products';
        END IF;
        
        IF sales_count = 0 THEN
            RAISE WARNING 'No sales in fact_sales';
        END IF;
    END $$;
    """,
    dag=dag,
)

# Task dependencies - Customers và Products chạy song song, sau đó Sales, rồi Summary và Quality Check
[extract_customers_task, extract_products_task] >> extract_sales_task >> create_summary_task >> data_quality_check 