#!/usr/bin/env python3
"""
Script để generate thêm sample data cho E-commerce database
Sử dụng để testing và demo
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker
import sys
import uuid

# Initialize Faker
fake = Faker()

# Database connection config
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'database': 'ecommerce',
    'user': 'datauser',
    'password': 'datapass'
}

def connect_db():
    """Kết nối tới PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Không thể kết nối database: {e}")
        sys.exit(1)

def generate_customers(num_customers=50):
    """Generate random customers với unique emails"""
    customers = []
    cities = ['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Can Tho', 'Nha Trang', 
              'Hue', 'Vung Tau', 'Phan Thiet', 'Dalat', 'Hai Phong']
    
    used_emails = set()
    
    for i in range(num_customers):
        # Tạo unique email
        base_email = fake.email()
        while base_email in used_emails:
            # Thêm random suffix để đảm bảo unique
            username, domain = base_email.split('@')
            base_email = f"{username}_{i}_{random.randint(1000, 9999)}@{domain}"
        
        used_emails.add(base_email)
        
        customer = {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': base_email,
            'phone': fake.phone_number()[:15],
            'address': fake.address().replace('\n', ', '),
            'city': random.choice(cities),
            'country': 'Vietnam'
        }
        customers.append(customer)
    
    return customers

def generate_products(num_products=100):
    """Generate random products"""
    products = []
    categories = ['Electronics', 'Furniture', 'Sports', 'Kitchenware', 'Books', 'Clothing']
    suppliers = ['TechCorp', 'FurnitureCo', 'SportsBrand', 'HomeGoods', 'BookStore', 'FashionHub']
    
    product_names = {
        'Electronics': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Tablet'],
        'Furniture': ['Chair', 'Desk', 'Lamp', 'Bookshelf', 'Sofa', 'Table'],
        'Sports': ['Running Shoes', 'Yoga Mat', 'Dumbbells', 'Basketball', 'Tennis Racket'],
        'Kitchenware': ['Coffee Mug', 'Plate Set', 'Blender', 'Toaster', 'Knife Set'],
        'Books': ['Programming Guide', 'Data Science Book', 'Novel', 'Cookbook', 'Biography'],
        'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress', 'Hat']
    }
    
    for i in range(num_products):
        category = random.choice(categories)
        product_name = random.choice(product_names[category])
        
        product = {
            'product_name': f"{product_name} {fake.word().title()} #{i+1}",
            'category': category,
            'price': round(random.uniform(10, 2000), 2),
            'stock_quantity': random.randint(0, 500),
            'supplier': random.choice(suppliers)
        }
        products.append(product)
    
    return products

def generate_orders_and_items(conn, num_orders=150):
    """Generate random orders và order items"""
    cursor = conn.cursor()
    
    # Get existing customers và products
    cursor.execute("SELECT customer_id FROM source_data.customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id, price FROM source_data.products")
    products = cursor.fetchall()
    
    if not customer_ids or not products:
        print("Cần có customers và products trước khi tạo orders")
        return [], []
    
    orders = []
    order_items = []
    
    for _ in range(num_orders):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-90d', end_date='now')
        status = random.choice(['pending', 'completed', 'cancelled', 'shipped'])
        payment_method = random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer'])
        
        # Generate order items
        num_items = random.randint(1, 5)
        selected_products = random.sample(products, min(num_items, len(products)))
        
        total_amount = 0
        order_id_placeholder = len(orders) + 1  # Temporary ID
        
        for product_id, price in selected_products:
            quantity = random.randint(1, 3)
            unit_price = price
            total_price = quantity * unit_price
            total_amount += total_price
            
            order_items.append({
                'order_id': order_id_placeholder,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price
            })
        
        orders.append({
            'customer_id': customer_id,
            'order_date': order_date,
            'status': status,
            'total_amount': round(total_amount, 2),
            'payment_method': payment_method,
            'shipping_address': fake.address().replace('\n', ', ')
        })
    
    return orders, order_items

def insert_data(conn, customers, products, orders, order_items):
    """Insert data vào database với conflict handling"""
    cursor = conn.cursor()
    
    try:
        # Insert customers với ON CONFLICT
        print(f"Inserting {len(customers)} customers...")
        success_count = 0
        for customer in customers:
            try:
                cursor.execute("""
                    INSERT INTO source_data.customers 
                    (first_name, last_name, email, phone, address, city, country)
                    VALUES (%(first_name)s, %(last_name)s, %(email)s, %(phone)s, 
                            %(address)s, %(city)s, %(country)s)
                    ON CONFLICT (email) DO NOTHING
                """, customer)
                if cursor.rowcount > 0:
                    success_count += 1
            except Exception as e:
                print(f"Skipped customer {customer['email']}: {e}")
                continue
        print(f"✅ Inserted {success_count} new customers")
        
        # Insert products
        print(f"Inserting {len(products)} products...")
        success_count = 0
        for product in products:
            try:
                cursor.execute("""
                    INSERT INTO source_data.products 
                    (product_name, category, price, stock_quantity, supplier)
                    VALUES (%(product_name)s, %(category)s, %(price)s, 
                            %(stock_quantity)s, %(supplier)s)
                """, product)
                success_count += 1
            except Exception as e:
                print(f"Skipped product {product['product_name']}: {e}")
                continue
        print(f"✅ Inserted {success_count} new products")
        
        # Insert orders và get real order IDs
        print(f"Inserting {len(orders)} orders...")
        order_id_mapping = {}
        success_count = 0
        for i, order in enumerate(orders):
            try:
                cursor.execute("""
                    INSERT INTO source_data.orders 
                    (customer_id, order_date, status, total_amount, payment_method, shipping_address)
                    VALUES (%(customer_id)s, %(order_date)s, %(status)s, 
                            %(total_amount)s, %(payment_method)s, %(shipping_address)s)
                    RETURNING order_id
                """, order)
                real_order_id = cursor.fetchone()[0]
                order_id_mapping[i + 1] = real_order_id
                success_count += 1
            except Exception as e:
                print(f"Skipped order: {e}")
                continue
        print(f"✅ Inserted {success_count} new orders")
        
        # Insert order items với real order IDs
        print(f"Inserting order items...")
        success_count = 0
        for item in order_items:
            if item['order_id'] in order_id_mapping:
                try:
                    item['order_id'] = order_id_mapping[item['order_id']]
                    cursor.execute("""
                        INSERT INTO source_data.order_items 
                        (order_id, product_id, quantity, unit_price, total_price)
                        VALUES (%(order_id)s, %(product_id)s, %(quantity)s, 
                                %(unit_price)s, %(total_price)s)
                    """, item)
                    success_count += 1
                except Exception as e:
                    print(f"Skipped order item: {e}")
                    continue
        print(f"✅ Inserted {success_count} new order items")
        
        conn.commit()
        print("✅ Data insertion completed!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting data: {e}")

def main():
    """Main function"""
    print("🚀 Generating sample data for E-commerce database...")
    
    # Connect to database
    conn = connect_db()
    
    # Generate data với số lượng hợp lý
    print("📊 Generating customers...")
    customers = generate_customers(30)  # Giảm từ 10000 xuống 50
    
    print("📦 Generating products...")
    products = generate_products(100)  # Giảm từ 10000 xuống 100
    
    print("🛒 Generating orders và order items...")
    orders, order_items = generate_orders_and_items(conn, 150)  # Giảm từ 10000 xuống 150
    
    # Insert data
    insert_data(conn, customers, products, orders, order_items)
    
    # Show summary
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM source_data.customers")
    total_customers = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM source_data.products")
    total_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM source_data.orders")
    total_orders = cursor.fetchone()[0]
    
    print(f"""
📈 Database Summary:
   - Total Customers: {total_customers}
   - Total Products: {total_products}
   - Total Orders: {total_orders}
   
🎉 Sample data generation completed!
💡 Bây giờ bạn có thể chạy Airflow ETL pipeline để xử lý data này.
    """)
    
    conn.close()

if __name__ == "__main__":
    main() 