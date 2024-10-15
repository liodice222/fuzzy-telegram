import subprocess
import config
import extract.Create_Tables
import extract.download_csv
import clean.remove_csv
import clean.upload_new_csv
import load.process_all_csv_2

def run_script(script_path):
    try:
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_path}: {e.stderr}")

if __name__ == "__main__":
    scripts = [
        'extract/Create_Tables.py',
        'extract/download_csv.py',
        'load/process_all_csv_2.py',
        'clean/upload_new_csv.py',
        'clean/remove_csv.py'
    ]

    for script in scripts:
        run_script(script)