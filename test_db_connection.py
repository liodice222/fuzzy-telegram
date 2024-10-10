import cx_Oracle

# Set the TNS_ADMIN environment variable
import os
os.environ['TNS_ADMIN'] = '/wallet'

# Database credentials
db_username = "admin"
db_password = "Funkytelegram123*"
db_dsn = "dep_low"  # e.g., "adw_high"

# Connect to the Oracle database
connection = cx_Oracle.connect(db_username, db_password, db_dsn)
print("Connected to the database")

# Close the connection
connection.close()