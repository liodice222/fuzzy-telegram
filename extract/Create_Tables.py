import cx_Oracle
import os

def table_exists(cursor, table_name):
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM user_tables
        WHERE table_name = '{table_name.upper()}'
    """)
    return cursor.fetchone()[0] > 0

def Create_Tables():
    user = os.environ.get('DB_USERNAME')
    password = os.environ.get('DB_PASSWORD')
    dsn = os.environ.get('DB_DSN')

    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = connection.cursor()

    tables = {
        'customers': '''
            CREATE TABLE customers (
                customer_id NUMBER PRIMARY KEY,
                first_name VARCHAR2(100),
                last_name VARCHAR2(100),
                email VARCHAR2(255),
                created_at DATE
            )''',
        'orders': '''
            CREATE TABLE orders (
                order_id NUMBER PRIMARY KEY,
                customer_id NUMBER,
                order_date DATE,
                total_amount NUMBER(10, 2),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )''',
        'products': '''
            CREATE TABLE products (
                product_id NUMBER PRIMARY KEY,
                product_name VARCHAR2(255),
                price NUMBER(10, 2)
            )''',
        'payments': '''
            CREATE TABLE payments (
                payment_id NUMBER PRIMARY KEY,  -- Ensures uniqueness for each payment
                order_id NUMBER,
                amount NUMBER(10, 2),
                payment_date DATE,
                FOREIGN KEY (order_id) REFERENCES orders(order_id)  -- Links payments to valid orders
            )'''
    }

    for table_name, create_statement in tables.items():
        if not table_exists(cursor, table_name):
            cursor.execute(create_statement)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()
