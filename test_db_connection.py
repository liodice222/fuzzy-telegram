import cx_Oracle
import os

# set environment variables
os.environ['TNS_ADMIN'] ='/home/opc/wallet'

# Database credentials
db_username = os.environ.get('DB_USERNAME')
db_password = os.environ.get('DB_PASSWORD')
db_dsn = os.environ.get('DB_DSN')
wallet_location = os.environ.get('TNS_ADMIN')

print(f"Username: {db_username}")
print(f"Password: {db_password}")
print(f"DSN: {db_dsn}")
print(f"Wallet Location: {wallet_location}")

try:
    # Connect to the Oracle database with a timeout
    connection = cx_Oracle.connect(db_username, db_password, db_dsn)
    print("Connected to the database")
    
    # Perform database operations here

except cx_Oracle.DatabaseError as e:
    error, = e.args
    print(f"Database connection failed: {error.message}")

finally:
    # Close the connection
    if 'connection' in locals() and connection:
        connection.close()
        print("Connection closed")
