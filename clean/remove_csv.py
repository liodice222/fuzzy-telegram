import oci
import os

# Define variables
BUCKET_NAME = 'DEP-ASH-OBJ-01'
DESTINATION_DIR = '/home/opc/downloads'

# Initialize OCI config and client
config_path = "/home/opc/.oci/config"
config = oci.config.from_file(file_location=config_path)
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Get namespace
NAMESPACE = object_storage_client.get_namespace().data


# List objects in the bucket
objects = object_storage_client.list_objects(NAMESPACE, BUCKET_NAME, fields='name').data.objects

#Delete csv files from instance


print("Download completed.")
for filename in os.listdir(DESTINATION_DIR):
    if filename.endswith('.csv'):
        file_path = os.path.join(DESTINATION_DIR, filename)
        try:
            os.remove(file_path)
            print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

print("CSV files deleted from the downloads directory.")