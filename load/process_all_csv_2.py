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

def process_csv_to_sql(csv_file_path, table_name, columns, date_columns=None, row_limit=10):
    """
    Processes a CSV file and generates SQL insert statements.
    
    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the table to insert data into.
    :param columns: List of column names.
    :param date_columns: List of column names that contain date values.
    :param row_limit: Number of rows to read from the CSV file.
    :return: List of SQL insert statements.
    """
    insert_statements = []
    date_columns = date_columns or []

    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header

        rows = [next(csv_reader) for _ in range(row_limit)]  # Read the specified number of lines

        for row in rows:
            values = []
            for i, column in enumerate(columns):
                if column in date_columns:
                    # Format date columns using TO_DATE
                    values.append(f"TO_DATE('{row[i]}', 'MM/DD/YYYY')")
                else:
                    # Handle other columns, escaping single quotes in string values
                    value = row[i].replace("'", "''") if isinstance(row[i], str) else row[i]
                    values.append(f"'{value}'" if isinstance(row[i], str) else row[i])
            
            values_str = ", ".join(values)
            insert_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_str});\n"
            insert_statements.append(insert_statement)
    
    return insert_statements

# Process each CSV file
insert_statements_customers = process_csv_to_sql(
    CSV_FILE_PATH_CUSTOMERS, 
    'customers', 
    ['customer_id', 'first_name', 'last_name', 'email', 'created_at'],
    date_columns=['created_at']
)

insert_statements_payments = process_csv_to_sql(
    CSV_FILE_PATH_PAYMENTS, 
    'payments', 
    ['payment_id', 'order_id', 'amount', 'payment_date'],
    date_columns=['payment_date']
)

insert_statements_orders = process_csv_to_sql(
    CSV_FILE_PATH_ORDERS, 
    'orders', 
    ['order_id', 'customer_id', 'order_date', 'total_amount'],
    date_columns=['order_date']
)

insert_statements_products = process_csv_to_sql(
    CSV_FILE_PATH_PRODUCTS, 
    'products', 
    ['product_id', 'product_name', 'price']
)

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
command = f"{SQLPLUS_PATH} {DB_USER}/{DB_PASSWORD}@{DB_DSN} @{sql_file_path}"
result = subprocess.run(command, shell=True, universal_newlines=True)

print(result.stdout)
print(result.stderr)

print("Data inserted into the ADB from all CSV files.")
