
import cx_Oracle

# Define database connection credentials
dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XE")
connection = cx_Oracle.connect(user="ADMIN", password="FuzzyTelegram123*", dsn=dsn)

# SQL commands to drop the tables if they exist
drop_tables_sql = '''
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE payments CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
'''

# Create a cursor and execute the drop commands
with connection.cursor() as cursor:
    cursor.execute(drop_tables_sql)

# Commit the changes and close the connection
connection.commit()
connection.close()
