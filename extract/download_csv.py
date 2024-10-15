import oci
import os

def download_csv():
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

    # Download each CSV file
    for obj in objects:
        if obj.name.endswith('.csv'):
            print(f"Downloading {obj.name}...")
            get_obj = object_storage_client.get_object(NAMESPACE, BUCKET_NAME, obj.name)
            with open(os.path.join(DESTINATION_DIR, obj.name), 'wb') as f:
                f.write(get_obj.data.content)

    print("Download completed.")


# if __name__ == "__main__":
#     download_csv()
