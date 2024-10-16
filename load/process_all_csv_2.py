import csv
import subprocess
import os

def process_csv_to_sql(csv_file_path, table_name, columns, date_columns=None, row_limit=1000):
    """
    Processes a CSV file and generates SQL insert statements.

    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the table to insert data into.
    :param columns: List of column names.
    :param date_columns: List of column names that contain date values.
    :param row_limit: Number of rows to read from the CSV file.
    :return: List of SQL insert statements.
    """
    print(f"Processing CSV file: {csv_file_path} for table: {table_name}")
    insert_statements = []
    date_columns = date_columns or []

    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header
        print(f"CSV header: {header}")

        # Read rows with exception handling for shorter files
        rows = []
        try:
            for _ in range(row_limit):
                rows.append(next(csv_reader))
        except StopIteration:
            print(f"End of file reached after reading {len(rows)} rows")

        for row in rows:
            values = []
            for i, column in enumerate(columns):
                if column in date_columns:
                    # Check if the date column is not empty
                    if row[i]:
                        values.append(f"TO_DATE('{row[i]}', 'MM/DD/YYYY')")
                    else:
                        values.append('NULL')  # Insert NULL if the date is empty
                else:
                    # Handle other columns, escaping single quotes in string values
                    value = row[i].replace("'", "''") if isinstance(row[i], str) else row[i]
                    values.append(f"'{value}'" if isinstance(row[i], str) else row[i])

            values_str = ", ".join(values)
            insert_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_str});\n"
            insert_statements.append(insert_statement)

    print(f"Generated {len(insert_statements)} SQL insert statements for table: {table_name}")
    return insert_statements


def process_and_load_data():
    # Define variables
    CSV_FILE_PATH_CUSTOMERS = '/home/opc/downloads/customers.csv'
    CSV_FILE_PATH_PAYMENTS = '/home/opc/downloads/payments.csv'
    CSV_FILE_PATH_ORDERS = '/home/opc/downloads/orders.csv'
    CSV_FILE_PATH_PRODUCTS = '/home/opc/downloads/products.csv'

    SQLPLUS_PATH = '/usr/lib/oracle/23/client64/bin/sqlplus'
    DB_USER = os.environ.get('DB_USERNAME')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_DSN = os.environ.get('DB_DSN')

    # Process each CSV file
    print("Processing customers CSV file...")
    insert_statements_customers = process_csv_to_sql(
        CSV_FILE_PATH_CUSTOMERS, 
        'customers', 
        ['customer_id', 'first_name', 'last_name', 'email', 'created_at'],
        date_columns=['created_at']
    )

    print("Processing orders CSV file...")
    insert_statements_orders = process_csv_to_sql(
        CSV_FILE_PATH_ORDERS, 
        'orders', 
        ['order_id', 'customer_id', 'order_date', 'total_amount'],
        date_columns=['order_date']
    )

    print("Processing payments CSV file...")
    insert_statements_payments = process_csv_to_sql(
        CSV_FILE_PATH_PAYMENTS, 
        'payments', 
        ['payment_id', 'order_id', 'amount', 'payment_date'],
        date_columns=['payment_date']
    )

    print("Processing products CSV file...")
    insert_statements_products = process_csv_to_sql(
        CSV_FILE_PATH_PRODUCTS, 
        'products', 
        ['product_id', 'product_name', 'price']
    )

    # Combine all insert statements into one SQL file
    sql_file_path = '/tmp/insert_data.sql'
    print(f"Writing SQL insert statements to file: {sql_file_path}")
    with open(sql_file_path, mode='w') as sql_file:
        sql_file.write("SET DEFINE OFF;\n")  # Disable variable substitution
        sql_file.writelines(insert_statements_customers)
        sql_file.writelines(insert_statements_orders)
        sql_file.writelines(insert_statements_payments)
        sql_file.writelines(insert_statements_products)
        sql_file.write("EXIT;\n")

    # Execute the SQL file using sqlplus
    command = f"{SQLPLUS_PATH} {DB_USER}/{DB_PASSWORD}@{DB_DSN} @{sql_file_path}"
    print(f"Executing SQL file using command: {command}")
    result = subprocess.run(command, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    print("SQL*Plus output:")
    print(result.stdout)
    print(result.stderr)

    if result.returncode == 0:
        print("Data inserted into the ADB from all CSV files successfully.")
    else:
        print("Error occurred while inserting data into the ADB.")


# if __name__ == "__main__":
#     process_and_load_data()
