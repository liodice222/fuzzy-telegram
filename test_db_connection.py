import cx_Oracle
import os

# Database credentials
db_username = "admin"
db_password = "Funkytelegram123*"

# DSN with connection timeout parameters
db_dsn = (
    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=your_host)(PORT=your_port))"
    "(CONNECT_DATA=(SERVICE_NAME=dep_low))"
    "(CONNECT_TIMEOUT=10)(RETRY_COUNT=3))"
)

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
