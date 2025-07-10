# E-commerce Analytics Pipeline - Bài Tập Thực Hành

Đây là một bài tập thực hành hoàn chỉnh về **Data Engineering Pipeline** sử dụng **Python**, **Apache Airflow**, **Grafana**, và **PostgreSQL**.

## 🎯 Mục Tiêu Học Tập

Thực hành xây dựng một **End-to-End Data Pipeline** với:

- **ETL Processing** với Apache Airflow
- **Data Visualization** với Grafana
- **Data Warehouse** với PostgreSQL
- **Monitoring & Alerting**

## 🏗️ Kiến Trúc Hệ Thống

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Source Data │───▶│   Airflow   │───▶│Data Warehouse│───▶│   Grafana   │
│ (PostgreSQL)│    │    ETL      │    │ (PostgreSQL) │    │ Dashboards  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## 📦 Các Thành Phần

### 1. **Apache Airflow**

- **Vai trò**: Orchestration platform - quản lý workflow ETL
- **Chức năng**:
  - Lập lịch chạy ETL jobs hàng ngày
  - Extract từ source tables
  - Transform data (customer segmentation, product categorization)
  - Load vào data warehouse
  - Data quality checks

### 2. **PostgreSQL**

- **Source Schema (`source_data`)**: Giả lập production database
  - `customers`, `products`, `orders`, `order_items`
- **Target Schema (`target_data`)**: Data warehouse
  - `dim_customers`, `dim_products`, `fact_sales`, `daily_sales_summary`

### 3. **Grafana**

- **Vai trò**: Data visualization và monitoring
- **Dashboards**:
  - KPI metrics (Revenue, Orders, Customers)
  - Time series charts
  - Category analysis
  - Customer insights

## 🚀 Cách Chạy Project

### Bước 1: Clone và Setup

```bash
# Clone project (nếu từ git)
git clone <repo-url>
cd Lotus

# Tạo environment file
echo 'AIRFLOW_UID=50000' > .env
echo 'AIRFLOW_GID=0' >> .env
```

### Bước 2: Khởi động Docker Services

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra status
docker-compose ps
```

### Bước 3: Setup Airflow Connection

1. Truy cập Airflow UI: `http://localhost:8080`
2. Login: `admin` / `admin`
3. Tạo Connection:
   - **Conn Id**: `postgres_data`
   - **Conn Type**: `Postgres`
   - **Host**: `postgres_data`
   - **Schema**: `ecommerce`
   - **Login**: `datauser`
   - **Password**: `datapass`
   - **Port**: `5432`

### Bước 4: Chạy ETL Pipeline

1. Trong Airflow UI, tìm DAG `ecommerce_etl_pipeline`
2. Enable DAG (toggle button)
3. Trigger DAG manually để test
4. Monitor task execution

### Bước 5: Truy cập Grafana Dashboard

1. Truy cập: `http://localhost:3000`
2. Login: `admin` / `admin`
3. Dashboard sẽ tự động load từ provisioning

## 📊 Sample Data

Project bao gồm sample data cho:

- **5 customers** từ các thành phố khác nhau
- **8 products** trong 4 categories (Electronics, Furniture, Sports, Kitchenware)
- **5 orders** với các trạng thái khác nhau
- **8 order items** chi tiết

## 🔍 Các Thao Tác Thực Hành

### 1. ETL Development

```python
# Customize transformation logic trong DAG
# File: airflow/dags/ecommerce_etl_dag.py

# Ví dụ: Thêm customer segmentation rule mới
def extract_and_transform_customers():
    # Modify customer segmentation logic
    extract_query = """
    SELECT customer_id,
           CASE 
               WHEN total_spent >= 1000 THEN 'VIP'
               WHEN total_spent >= 500 THEN 'Premium'
               ELSE 'Regular'
           END as customer_segment
    FROM ...
    """
```

### 2. Data Quality Checks

```sql
-- Thêm data quality rules
SELECT 
    COUNT(*) as total_customers,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as missing_emails,
    COUNT(CASE WHEN customer_segment IS NULL THEN 1 END) as missing_segments
FROM target_data.dim_customers;
```

### 3. Dashboard Customization

- Thêm panels mới trong Grafana
- Tạo alerts cho metrics quan trọng
- Customize visualization types

### 4. Database Operations

```bash
# Connect vào PostgreSQL
docker exec -it <postgres_container> psql -U datauser -d ecommerce

# Explore data
\dt source_data.*
\dt target_data.*

# Query examples
SELECT * FROM target_data.daily_sales_summary;
```

## 🎯 Bài Tập Mở Rộng

### Level 1: Beginner

1. **Thêm sản phẩm mới** vào source database
2. **Chạy lại ETL** và quan sát kết quả
3. **Tạo dashboard panel mới** cho top selling products

### Level 2: Intermediate

1. **Thêm DAG mới** để import data từ CSV file
2. **Tạo SLA alerts** trong Airflow cho tasks chạy quá lâu
3. **Implement incremental loading** thay vì full refresh

### Level 3: Advanced

1. **Thêm multiple data sources** (MySQL, Oracle simulation)
2. **Implement change data capture** (CDC) pattern
3. **Create automated testing** cho ETL logic
4. **Setup monitoring alerts** trong Grafana

## 🔧 Troubleshooting

### Common Issues:

1. **Airflow tasks fail**:

   ```bash
   # Check logs
   docker logs <airflow_container>

   # Reset DB
   docker-compose down -v
   docker-compose up -d
   ```
2. **Database connection issues**:

   ```bash
   # Test connection
   docker exec -it <postgres_container> pg_isready -U datauser
   ```
3. **Grafana dashboard không hiển thị data**:

   - Kiểm tra PostgreSQL connection trong Grafana
   - Verify data có trong target tables
   - Check dashboard time range

## 📚 Learning Resources

### Airflow Concepts:

- **DAG**: Directed Acyclic Graph - workflow definition
- **Task**: Individual unit of work
- **Operator**: Defines what task does (PythonOperator, SQLOperator)
- **Hook**: Interface to external systems (PostgresHook)

### ETL Best Practices:

- **Idempotency**: ETL should produce same result when re-run
- **Data Quality**: Always validate data after transformation
- **Monitoring**: Track success/failure rates và execution time
- **Documentation**: Comment code và maintain data lineage

### Performance Tips:

- **Indexing**: Create indexes on frequently queried columns
- **Partitioning**: Partition large tables by date
- **Parallel Processing**: Run independent tasks in parallel
- **Connection Pooling**: Reuse database connections

## 🌟 Next Steps

Sau khi hoàn thành bài tập này, bạn có thể:

1. **Học về real-time streaming** với Apache Kafka
2. **Implement cloud deployment** với AWS/GCP/Azure
3. **Explore advanced analytics** với dbt (data build tool)
4. **Study MLOps pipeline** integration

## 📞 Support

Nếu gặp vấn đề trong quá trình thực hành, hãy:

1. Check logs trong Docker containers
2. Verify database connections
3. Review Airflow task logs
4. Test individual components

---

## How to run gerage data
```py
cd ~/Desktop/Lotus
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary faker
python scripts/generate_sample_data.py
```


**Happy Learning! 🚀**

*Bài tập này giúp bạn hiểu được end-to-end data pipeline trong thực tế và cách các công cụ làm việc cùng nhau.*
