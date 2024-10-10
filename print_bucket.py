import oci
import pandas as pd
from io import BytesIO

# Configuration
config_path = "/home/opc/.oci/config"
bucket_name = "your_bucket_name"
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

# Iterate over each object and read the first five rows
for obj in objects:
    file_name = obj.name
    try:
        print(f"\nReading {file_name} from bucket {bucket_name}...")
        get_obj_response = object_storage_client.get_object(namespace, bucket_name, file_name)
        file_content = get_obj_response.data.content
        df = pd.read_csv(BytesIO(file_content))
        print(f"First 5 rows of {file_name}:\n{df.head()}")
    except oci.exceptions.ServiceError as e:
        print(f"Error reading {file_name}: {e}")
    except Exception as e:
        print(f"Error processing {file_name}: {e}")