import cx_Oracle
import os

# Autonomous Database connection info
user = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
dsn = os.environ.get('DB_DSN')
def create_tables():
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = connection.cursor()

    # Creating Customers Table
    cursor.execute('''
    CREATE TABLE customers (
        customer_id NUMBER PRIMARY KEY,
        first_name VARCHAR2(100),
        last_name VARCHAR2(100),
        email VARCHAR2(255),
        created_at DATE
    )''')

    # Creating Orders Table
    cursor.execute('''
    CREATE TABLE orders (
        order_id NUMBER PRIMARY KEY,
        customer_id NUMBER,
        order_date DATE,
        total_amount NUMBER(10, 2),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )''')

    # Creating Products Table
    cursor.execute('''
    CREATE TABLE products (
        product_id NUMBER PRIMARY KEY,
        product_name VARCHAR2(255),
        price NUMBER(10, 2)
    )''')

    # Creating Payments Table
    cursor.execute('''
    CREATE TABLE payments (
        payment_id NUMBER PRIMARY KEY,
        order_id NUMBER,
        amount NUMBER(10, 2),
        payment_date DATE,
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )''')

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    create_tables()
    print('Tables Created.')
