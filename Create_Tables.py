import cx_Oracle
import os

# Autonomous Database connection info
user = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
dsn = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=lqnycsa0.adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=s5bmgthkjolzqu8_dep_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=no)))"

def create_tables():
    print("user:", user, "/n")
    print("password:", password, "/n")
    print("dsn:", dsn, "/n")
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
