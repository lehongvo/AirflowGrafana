-- Create schemas
CREATE SCHEMA IF NOT EXISTS source_data;
CREATE SCHEMA IF NOT EXISTS target_data;

-- Source tables (giả lập production database)
CREATE TABLE IF NOT EXISTS source_data.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_data.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INTEGER,
    supplier VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_data.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES source_data.customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(30),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_data.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES source_data.orders(order_id),
    product_id INTEGER REFERENCES source_data.products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Target tables (data warehouse - đã được transform)
CREATE TABLE IF NOT EXISTS target_data.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE,
    full_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    customer_segment VARCHAR(20),
    created_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS target_data.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    price_tier VARCHAR(20),
    stock_status VARCHAR(20),
    supplier VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS target_data.fact_sales (
    sale_key SERIAL PRIMARY KEY,
    order_id INTEGER,
    customer_key INTEGER REFERENCES target_data.dim_customers(customer_key),
    product_key INTEGER REFERENCES target_data.dim_products(product_key),
    sale_date DATE,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS target_data.daily_sales_summary (
    summary_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(12, 2),
    total_customers INTEGER,
    avg_order_value DECIMAL(10, 2),
    top_selling_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data vào source tables
INSERT INTO source_data.customers (first_name, last_name, email, phone, address, city, country) VALUES
('John', 'Doe', 'john.doe@email.com', '+1234567890', '123 Main St', 'New York', 'USA'),
('Jane', 'Smith', 'jane.smith@email.com', '+1234567891', '456 Oak Ave', 'Los Angeles', 'USA'),
('Mike', 'Johnson', 'mike.johnson@email.com', '+1234567892', '789 Pine Rd', 'Chicago', 'USA'),
('Sarah', 'Wilson', 'sarah.wilson@email.com', '+1234567893', '321 Elm St', 'Houston', 'USA'),
('David', 'Brown', 'david.brown@email.com', '+1234567894', '654 Maple Dr', 'Phoenix', 'USA');

INSERT INTO source_data.products (product_name, category, price, stock_quantity, supplier) VALUES
('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'TechCorp'),
('Wireless Mouse', 'Electronics', 29.99, 200, 'TechCorp'),
('Office Chair', 'Furniture', 199.99, 75, 'FurnitureCo'),
('Coffee Mug', 'Kitchenware', 12.99, 500, 'HomeGoods'),
('Running Shoes', 'Sports', 89.99, 100, 'SportsBrand'),
('Smartphone', 'Electronics', 699.99, 80, 'MobileTech'),
('Desk Lamp', 'Furniture', 45.99, 120, 'FurnitureCo'),
('Water Bottle', 'Sports', 24.99, 300, 'SportsBrand');

INSERT INTO source_data.orders (customer_id, status, total_amount, payment_method, shipping_address) VALUES
(1, 'completed', 1329.98, 'credit_card', '123 Main St, New York, USA'),
(2, 'completed', 199.99, 'paypal', '456 Oak Ave, Los Angeles, USA'),
(3, 'pending', 89.99, 'credit_card', '789 Pine Rd, Chicago, USA'),
(4, 'completed', 57.98, 'debit_card', '321 Elm St, Houston, USA'),
(5, 'completed', 745.98, 'credit_card', '654 Maple Dr, Phoenix, USA');

INSERT INTO source_data.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 1299.99, 1299.99),
(1, 2, 1, 29.99, 29.99),
(2, 3, 1, 199.99, 199.99),
(3, 5, 1, 89.99, 89.99),
(4, 4, 2, 12.99, 25.98),
(4, 7, 1, 45.99, 45.99),
(5, 6, 1, 699.99, 699.99),
(5, 8, 1, 24.99, 24.99);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON source_data.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON source_data.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON source_data.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON source_data.order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON target_data.fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON target_data.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON target_data.fact_sales(product_key); 