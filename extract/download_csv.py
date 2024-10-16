import oci
import os

# TODO: create if statement to make sure files are not already downloaded

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
            destination_path = os.path.join(DESTINATION_DIR, obj.name)
            if not os.path.exists(destination_path):
                print(f"Downloading {obj.name}...")
                get_obj = object_storage_client.get_object(NAMESPACE, BUCKET_NAME, obj.name)
                with open(destination_path, 'wb') as f:
                    f.write(get_obj.data.content)
                print(f"Downloaded {obj.name} to {destination_path}")
            else:
                print(f"File {obj.name} already exists at {destination_path}, skipping download.")

    print("Download completed.")

# if __name__ == "__main__":
#     download_csv()
