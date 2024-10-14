import oci
import pandas as pd
from io import BytesIO
import subprocess
import os

# Configuration
config_path = "/home/opc/.oci/config"
bucket_name = "DEP-ASH-OBJ-01"
compartment_name = "Data_Engineering_Project"

# Initialize the OCI configuration
config = oci.config.from_file(file_location=config_path)

# Create a service client for Object Storage
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Get the namespace
namespace = object_storage_client.get_namespace().data

# List all compartments accessible by the user
identity_client = oci.identity.IdentityClient(config)
compartment_id = config["tenancy"]  # Root compartment (tenancy)

compartments = oci.pagination.list_call_get_all_results(
    identity_client.list_compartments,
    compartment_id,
    compartment_id_in_subtree=True
).data

# Add root compartment to the list of compartments
compartments.append(oci.identity.models.Compartment(id=compartment_id))

# Find the specified compartment
compartment = next((c for c in compartments if c.name == compartment_name), None)
if not compartment:
    raise ValueError(f"Compartment {compartment_name} not found")

# List buckets in the specified compartment
buckets = oci.pagination.list_call_get_all_results(
    object_storage_client.list_buckets,
    namespace,
    compartment.id
).data

# Check if the specified bucket exists
bucket = next((b for b in buckets if b.name == bucket_name), None)
if not bucket:
    raise ValueError(f"Bucket {bucket_name} not found in compartment {compartment_name}")

# List all objects in the specified bucket
objects = oci.pagination.list_call_get_all_results(
    object_storage_client.list_objects,
    namespace,
    bucket_name
).data.objects

# SQL statement to create the customers table
create_table_sql = """
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(255),
    last_name VARCHAR2(255),
    email VARCHAR2(255),
    created_at DATE
)
"""

# Function to execute SQL commands using sqlplus
def execute_sqlplus_command(sql_command):
    try:
        subprocess.run(
            [
                "sudo", "sqlplus",
                f"{os.environ['DB_USERNAME']}/{os.environ['DB_PASSWORD']}@(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=lqnycsa0.adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=s5bmgthkjolzqu8_dep_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=no))",
                f"@{sql_command}"
            ],
            check=True
        )
        print("SQL command executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing SQL command: {e}")

# Execute the SQL statement to create the customers table
execute_sqlplus_command(create_table_sql)

# Find the 'customers.csv' file
file_name = 'customers.csv'
file_obj = next((obj for obj in objects if obj.name == file_name), None)

if not file_obj:
    raise ValueError(f"File {file_name} not found in bucket {bucket_name}")

# Read the 'customers.csv' file
try:
    get_obj_response = object_storage_client.get_object(namespace, bucket_name, file_name)
    file_content = get_obj_response.data.content
    df = pd.read_csv(BytesIO(file_content))

    # Generate SQL INSERT statements for each row in the DataFrame
    insert_statements = []
    for _, row in df.iterrows():
        values = ', '.join([f"'{str(value).replace("'", "''")}'" if isinstance(value, str) else str(value) for value in row])
        insert_sql = f"INSERT INTO customers VALUES ({values});"
        insert_statements.append(insert_sql)

    # Execute each INSERT statement
    for insert_sql in insert_statements:
        execute_sqlplus_command(insert_sql)

    print(f"Data inserted into table {table_name} successfully.")

except oci.exceptions.ServiceError as e:
    print(f"Error reading {file_name}: {e}")
except Exception as e:
    print(f"Error processing {file_name}: {e}")
