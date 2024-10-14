import csv
import subprocess
import os

# Define variables
CSV_FILE_PATH_CUSTOMERS = '/home/opc/downloads/customers.csv'
CSV_FILE_PATH_PAYMENTS = '/home/opc/downloads/payments.csv'
CSV_FILE_PATH_ORDERS = '/home/opc/downloads/orders.csv'
CSV_FILE_PATH_PRODUCTS = '/home/opc/downloads/products.csv'

SQLPLUS_PATH = '/usr/lib/oracle/23/client64/bin/sqlplus'  
DB_USER = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_DSN = os.environ.get('DB_DSN')

def process_csv_to_sql(csv_file_path, table_name, columns, row_limit=10):
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header
        rows = [next(csv_reader) for _ in range(row_limit)]  # Read the first ten lines
    
    insert_statements = []
    for row in rows:
        try:
            values = ", ".join([f"'{value}'" for value in row])
            insert_statements.append(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});\n")
        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Exception: {e}")
    
    return insert_statements

# Process each CSV file
insert_statements_customers = process_csv_to_sql(CSV_FILE_PATH_CUSTOMERS, 'customers', ['customer_id', 'first_name', 'last_name', 'email', 'created_at'])
insert_statements_payments = process_csv_to_sql(CSV_FILE_PATH_PAYMENTS, 'payments', ['payment_id', 'order_id', 'amount', 'payment_date'])
insert_statements_orders = process_csv_to_sql(CSV_FILE_PATH_ORDERS, 'orders', ['order_id', 'customer_id', 'order_date', 'total_amount'])
insert_statements_products = process_csv_to_sql(CSV_FILE_PATH_PRODUCTS, 'products', ['product_id', 'product_name', 'price'])

# Combine all insert statements into one SQL file
sql_file_path = '/tmp/insert_data.sql'
with open(sql_file_path, mode='w') as sql_file:
    sql_file.write("SET DEFINE OFF;\n")  # Disable variable substitution
    sql_file.writelines(insert_statements_customers)
    sql_file.writelines(insert_statements_payments)
    sql_file.writelines(insert_statements_orders)
    sql_file.writelines(insert_statements_products)
    sql_file.write("EXIT;\n")  

# Execute the SQL file using sqlplus
command = f"{SQLPLUS_PATH} {DB_USER}/{DB_PASSWORD}@{DB_DSN} @ {sql_file_path}"
result = subprocess.run(command, shell=True, capture_output=True, text=True)

print(result.stdout)
print(result.stderr)

print("Data inserted into the ADB from all CSV files.")
