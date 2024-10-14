import csv
import os
import oci

# Define variables
CSV_FILE_PATH = '/home/opc/downloads/customers.csv'
NEW_CSV_FILE_PATH = '/home/opc/downloads/customers_updated.csv'
BUCKET_NAME = 'DEP-ASH-OBJ-01'
DESTINATION_DIR = '/home/opc/downloads'

# Initialize OCI config and client
config_path = "/home/opc/.oci/config"
config = oci.config.from_file(file_location=config_path)
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Get namespace
NAMESPACE = object_storage_client.get_namespace().data


# Read the first ten lines from the CSV file
with open(CSV_FILE_PATH, mode='r') as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)  # Skip the header
    rows = [next(csv_reader) for _ in range(10)]  # Read the first ten lines

# Read the entire CSV file and remove the first ten lines
with open(CSV_FILE_PATH, mode='r') as file:
    csv_reader = csv.reader(file)
    all_rows = list(csv_reader)

# Write the remaining rows to a new CSV file
with open(NEW_CSV_FILE_PATH, mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(header)  # Write the header
    csv_writer.writerows(all_rows[11:])  # Write the remaining rows

# Delete the old CSV file
try:
    os.remove(CSV_FILE_PATH)
    print(f"Deleted {CSV_FILE_PATH}")
except Exception as e:
    print(f"Error deleting {CSV_FILE_PATH}: {e}")

# Upload the new CSV file to object storage
try:
    with open(NEW_CSV_FILE_PATH, 'rb') as file:
        object_storage_client.put_object(
            NAMESPACE,
            BUCKET_NAME,
            os.path.basename(NEW_CSV_FILE_PATH),
            file
        )
    print(f"Uploaded {NEW_CSV_FILE_PATH} to bucket {BUCKET_NAME}")
except Exception as e:
    print(f"Error uploading {NEW_CSV_FILE_PATH}: {e}")

# Delete the new CSV file from the local directory
try:
    os.remove(NEW_CSV_FILE_PATH)
    print(f"Deleted {NEW_CSV_FILE_PATH}")
except Exception as e:
    print(f"Error deleting {NEW_CSV_FILE_PATH}: {e}")

print("CSV processing and upload completed.")
