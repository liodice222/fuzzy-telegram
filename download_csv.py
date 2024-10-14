import oci
import os

# Define variables
BUCKET_NAME = 'DEP-ASH-OBJ-01'
NAMESPACE = object_storage_client.get_namespace().dat
DESTINATION_DIR = '/home/opc/downloads'


# Initialize OCI config and client
config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# List objects in the bucket
objects = object_storage_client.list_objects(NAMESPACE, BUCKET_NAME, fields='name').data.objects

# Download each CSV file
for obj in objects:
    if obj.name.endswith('.csv'):
        print(f"Downloading {obj.name}...")
        get_obj = object_storage_client.get_object(NAMESPACE, BUCKET_NAME, obj.name)
        with open(os.path.join(DESTINATION_DIR, obj.name), 'wb') as f:
            f.write(get_obj.data.content)

print("Download completed.")
