import oci
import os

# Define variables
BUCKET_NAME = 'DEP-ASH-OBJ-01'
DESTINATION_DIR = '/home/opc/downloads'
FILES_TO_DELETE = ['customers.csv', 'products.csv', 'orders.csv', 'payments.csv']

# Initialize OCI config and client
config_path = "/home/opc/.oci/config"
config = oci.config.from_file(file_location=config_path)
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Get namespace
NAMESPACE = object_storage_client.get_namespace().data

# List objects in the bucket
objects = object_storage_client.list_objects(NAMESPACE, BUCKET_NAME, fields='name').data.objects

# Delete specified files in the bucket
for obj in objects:
    if obj.name in FILES_TO_DELETE:
        try:
            object_storage_client.delete_object(NAMESPACE, BUCKET_NAME, obj.name)
            print(f"Deleted {obj.name} from bucket {BUCKET_NAME}")
        except Exception as e:
            print(f"Error deleting {obj.name} from bucket {BUCKET_NAME}: {e}")

# Delete specified CSV files from the instance
for filename in os.listdir(DESTINATION_DIR):
    if filename in FILES_TO_DELETE:
        file_path = os.path.join(DESTINATION_DIR, filename)
        try:
            os.remove(file_path)
            print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

print("Specified CSV files deleted from the downloads directory and the bucket.")
