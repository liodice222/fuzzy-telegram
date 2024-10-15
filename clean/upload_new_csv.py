import csv
import os
import oci

def process_and_upload_all_csvs():
    # Define variables
    CSV_FILE_PATHS = {
        'customers': '/home/opc/downloads/customers.csv',
        'payments': '/home/opc/downloads/payments.csv',
        'orders': '/home/opc/downloads/orders.csv',
        'products': '/home/opc/downloads/products.csv'
    }

    BUCKET_NAME = 'DEP-ASH-OBJ-01'

    # Initialize OCI config and client
    config_path = "/home/opc/.oci/config"
    config = oci.config.from_file(file_location=config_path)
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # Get namespace
    NAMESPACE = object_storage_client.get_namespace().data

    def process_and_upload_csv(csv_file_path):
        # Read the first ten lines from the CSV file
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Skip the header
            rows = [next(csv_reader) for _ in range(10)]  # Read the first ten lines

        # Read the entire CSV file and remove the first ten lines
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.reader(file)
            all_rows = list(csv_reader)

        # Write the remaining rows to a new CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(header)  # Write the header
            csv_writer.writerows(all_rows[11:])  # Write the remaining rows

        # Delete the old CSV file
        try:
            os.remove(csv_file_path)
            print(f"Deleted {csv_file_path}")
        except Exception as e:
            print(f"Error deleting {csv_file_path}: {e}")

        # Upload the new CSV file to object storage
        try:
            with open(csv_file_path, 'rb') as file:
                object_storage_client.put_object(
                    NAMESPACE,
                    BUCKET_NAME,
                    os.path.basename(csv_file_path),
                    file
                )
            print(f"Uploaded {csv_file_path} to bucket {BUCKET_NAME}")
        except Exception as e:
            print(f"Error uploading {csv_file_path}: {e}")

        # Delete the new CSV file from the local directory
        try:
            os.remove(csv_file_path)
            print(f"Deleted {csv_file_path}")
        except Exception as e:
            print(f"Error deleting {csv_file_path}: {e}")

    # Process and upload each CSV file
    for key in CSV_FILE_PATHS:
        process_and_upload_csv(CSV_FILE_PATHS[key])

    print("CSV processing and upload completed.")

# Uncomment the following lines if you want to run this script standalone
# if __name__ == "__main__":
#     process_and_upload_all_csvs()
