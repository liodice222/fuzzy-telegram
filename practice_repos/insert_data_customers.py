import csv
import subprocess
import os

# Define variables
CSV_FILE_PATH = '/home/opc/downloads/customers.csv'
SQLPLUS_PATH = '/usr/lib/oracle/23/client64/bin/sqlplus'  
DB_USER = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_DSN = os.environ.get('DB_DSN')

# Read the first ten lines from the CSV file
with open(CSV_FILE_PATH, mode='r') as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)  # Skip the header
    rows = [next(csv_reader) for _ in range(1000)]  # Read the first ten lines

# Debugging: Print the rows to check the date format
print("First ten rows from the CSV file:")
for row in rows:
    print(row)

# Generate SQL insert statements
insert_statements = []
for row in rows:
    try:
        insert_statements.append(
            f"INSERT INTO customers (customer_id, first_name, last_name, email, created_at) "
            f"VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', TO_DATE('{row[4]}', 'MM/DD/YYYY'));\n"
        )
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Exception: {e}")
        
# Add a query to show the inserted records
query_statements = "SELECT * FROM customers WHERE customer_id IN ("
query_statements += ", ".join([row[0] for row in rows])
query_statements += ");\n"

# Write the SQL statements to a temporary SQL file
sql_file_path = '/tmp/insert_customers.sql'
with open(sql_file_path, mode='w') as sql_file:
    sql_file.write("SET DEFINE OFF;\n")  # Disable variable substitution
    sql_file.writelines(insert_statements)
    sql_file.write(query_statements)  
    sql_file.write("EXIT;\n")  

# Execute the SQL file using sqlplus
command = f"{SQLPLUS_PATH} {DB_USER}/{DB_PASSWORD}@{DB_DSN} @ {sql_file_path}"
result = subprocess.run(command, shell=True, capture_output=True, text=True)

print(result.stdout)
print(result.stderr)

print("First ten lines inserted into the ADW.")
