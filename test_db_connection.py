import cx_Oracle
import os

# Database credentials
db_username = "admin"
db_password = "Fuzzytelegram123*"
db_dsn = "dep_low"  # e.g., "adw_high"

# Connection parameters
connection_params = {
    "user": db_username,
    "password": db_password,
    "dsn": db_dsn,
    "encoding": "UTF-8",
    "nencoding": "UTF-8",
    "timeout": 10  # Timeout in seconds
}

try:
    # Connect to the Oracle database with a timeout
    connection = cx_Oracle.connect(**connection_params)
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
