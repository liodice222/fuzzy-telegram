import oci
import pandas as pd
import cx_Oracle
from io import StringIO
import os

# OCI configuration
config_path = "/home/opc/.oci/config"
bucket_name = "DEP-ASH-OBJ-01"
compartment_name = "Data_Engineering_Project"
file_name = "customers.csv"

config = oci.config.from_file(file_location=config_path)
object_storage_client = oci.object_storage.ObjectStorageClient(config)
namespace = object_storage_client.get_namespace().data

# List compartments and find the specified one
identity_client = oci.identity.IdentityClient(config)
compartment_id = config["tenancy"]

compartments = oci.pagination.list_call_get_all_results(
    identity_client.list_compartments, compartment_id, compartment_id_in_subtree=True
).data
compartments.append(oci.identity.models.Compartment(id=compartment_id))

compartment = next((c for c in compartments if c.name == compartment_name), None)
if not compartment:
    raise ValueError(f"Compartment {compartment_name} not found")

# List buckets and find the specified one
buckets = oci.pagination.list_call_get_all_results(
    object_storage_client.list_buckets, namespace, compartment.id
).data

bucket = next((b for b in buckets if b.name == bucket_name), None)
if not bucket:
    raise ValueError(f"Bucket {bucket_name} not found in compartment {compartment_name}")

# Get the object from the bucket
object_name = file_name
csv_object = object_storage_client.get_object(namespace, bucket_name, object_name)

# Read the CSV file into a DataFrame
csv_content = csv_object.data.content.decode('utf-8')
df = pd.read_csv(StringIO(csv_content))

# Oracle database connection details
dsn = "(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=lqnycsa0.adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=s5bmgthkjolzqu8_dep_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=no)))"
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

# Establish the connection
connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
cursor = connection.cursor()

# Define the SQL insert statement
insert_sql = """
INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'))
"""

# Iterate through the DataFrame and insert each row
for index, row in df.iterrows():
    cursor.execute(insert_sql, (row['customer_id'], row['first_name'], row['last_name'], row['email'], row['created_at']))

# Commit the transaction
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
