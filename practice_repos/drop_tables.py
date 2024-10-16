import cx_Oracle
import os

# Define database connection credentials
user = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
dsn = os.environ.get('DB_DSN')
connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)

# SQL commands to drop the tables if they exist
drop_tables_sql = [
    "DROP TABLE customers CASCADE CONSTRAINTS",
    "DROP TABLE orders CASCADE CONSTRAINTS",
    "DROP TABLE payments CASCADE CONSTRAINTS",
    "DROP TABLE products CASCADE CONSTRAINTS"
]

# Create a cursor and execute the drop commands one by one
with connection.cursor() as cursor:
    for sql in drop_tables_sql:
        try:
            cursor.execute(sql)
            print(f"Executed: {sql}")
        except cx_Oracle.DatabaseError as e:
            error_obj, = e.args
            if error_obj.code == 942:  # ORA-00942: table or view does not exist
                print(f"Table does not exist: {sql}")
            else:
                print(f"Error executing {sql}: {e}")
                raise

# Commit the changes and close the connection
connection.commit()
connection.close()
