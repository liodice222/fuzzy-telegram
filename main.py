import subprocess


#import modules for extraction 
from extract.Create_Tables import Create_Tables
from extract.download_csv import download_csv

#import modules for loading 
from load.process_all_csv_2 import process_csv_to_sql 
from load.process_all_csv_2 import process_and_load_data

#import modules for cleaning 
from clean.remove_csv import delete_specified_files
from clean.upload_new_csv import process_and_upload_all_csvs





def run_script(script_path):
    try:
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_path}: {e.stderr}")

if __name__ == "__main__":
    print("Creating tables...")
    Create_Tables()
    print("Tables Created.")
    
    print("Starting extraction...")
    download_csv()

    print("Starting processing and laoding data...")
    process_and_load_data()
    
    print("Starting upload of new CS to Object Storage...")
    process_and_upload_all_csvs()
    
    print("Starting removal of old CSV from instance...")
    delete_specified_files()