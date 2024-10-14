import oci

# Initialize the OCI configuration
config = oci.config.from_file()  # Default location is ~/.oci/config

# Create a service client for Object Storage
object_storage_client = oci.object_storage.ObjectStorageClient(config)

try:
    # Get the namespace
    namespace = object_storage_client.get_namespace().data
    print(f"Namespace: {namespace}")
except oci.exceptions.ServiceError as e:
    print(f"Service error: {e}")