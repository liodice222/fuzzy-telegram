import oci
import pandas as pd
import cx_Oracle
from io import BytesIO

# Configuration
config_path = "/home/opc/.oci/config"
bucket_name = "DEP-ASH-ADW-01"
compartment_name = "Data_Engineering_Project"
db_username = "admin"
db_password = "Fuzzytelegram123*"
db_dsn = "dep_low"  # e.g., "hostname:port/service_name"

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

# Connect to the Oracle database
connection = cx_Oracle.connect(db_username, db_password, db_dsn)
cursor = connection.cursor()

# Function to generate SQL CREATE TABLE statement
def generate_create_table_sql(table_name, df):
    columns = []
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            col_type = "NUMBER"
        elif pd.api.types.is_float_dtype(df[col]):
            col_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(df[col]):
            col_type = "CHAR(1)"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            col_type = "DATE"
        else:
            col_type = "VARCHAR2(255)"
        columns.append(f"{col} {col_type}")
    columns_sql = ", ".join(columns)
    return f"CREATE TABLE {table_name} ({columns_sql})"

# Iterate over each object and create an empty SQL table
for obj in objects:
    file_name = obj.name
    table_name = file_name.split('.')[0]  # Use file name without extension as table name
    try:
        print(f"\nReading {file_name} from bucket {bucket_name}...")
        get_obj_response = object_storage_client.get_object(namespace, bucket_name, file_name)
        file_content = get_obj_response.data.content
        df = pd.read_csv(BytesIO(file_content))
        create_table_sql = generate_create_table_sql(table_name, df)
        print(f"Creating table {table_name}...")
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"Table {table_name} created successfully.")
    except oci.exceptions.ServiceError as e:
        print(f"Error reading {file_name}: {e}")
    except Exception as e:
        print(f"Error processing {file_name}: {e}")

# Close the database connection
cursor.close()
connection.close()
