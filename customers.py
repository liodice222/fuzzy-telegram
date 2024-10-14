import subprocess
import oci
import pandas as pd
from io import BytesIO
import os

# OCI configuration
config_path = "/home/opc/.oci/config"
bucket_name = "DEP-ASH-OBJ-01"
compartment_name = "Data_Engineering_Project"
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

# List objects in the specified bucket
objects = oci.pagination.list_call_get_all_results(
    object_storage_client.list_objects, namespace, bucket_name
).data.objects

# SQL commands to create tables
create_tables_sql = [
    """
    CREATE TABLE customers (
        customer_id NUMBER PRIMARY KEY,
        first_name VARCHAR2(255),
        last_name VARCHAR2(255),
        email VARCHAR2(255),
        created_at DATE
    )
    """,
    """
    CREATE TABLE orders (
        order_id NUMBER PRIMARY KEY,
        customer_id NUMBER,
        order_date DATE,
        total_amount NUMBER(10, 2),
        CONSTRAINT fk_customer
            FOREIGN KEY (customer_id)
            REFERENCES customers(customer_id)
    )
    """,
    """
    CREATE TABLE products (
        product_id NUMBER PRIMARY KEY,
        product_name VARCHAR2(255),
        price NUMBER(10,2)
    )
    """,
    """
    CREATE TABLE payments (
        payment_id NUMBER PRIMARY KEY,
        order_id NUMBER,
        amount NUMBER(10,2),
        payment_date DATE,
        CONSTRAINT fk_order
            FOREIGN KEY (order_id)
            REFERENCES orders(order_id)
    )
    """
]

# Write SQL commands to a file
with open("create_tables.sql", "w") as sql_file:
    for sql in create_tables_sql:
        sql_file.write(sql + "\n")

# Write the sqlplus command to a shell script
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
connection_string = "(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=lqnycsa0.adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=s5bmgthkjolzqu8_dep_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=no)))"

with open("run_sqlplus.sh", "w") as script_file:
    script_file.write(f"#!/bin/bash\n")
    script_file.write(f"echo 'exit' | sqlplus {db_username}/{db_password}@'{connection_string}' @create_tables.sql\n")

# Make the shell script executable
subprocess.run(["chmod", "+x", "run_sqlplus.sh"], check=True)

# Execute the shell script
subprocess.run(["sudo", "./run_sqlplus.sh"], check=True)
