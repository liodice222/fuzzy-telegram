import os

# Configuration settings
DB_USER = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_DSN = os.environ.get('DB_DSN')
SQLPLUS_PATH = '/usr/lib/oracle/23/client64/bin/sqlplus'
BUCKET_NAME = 'DEP-ASH-OBJ-01'
DESTINATION_DIR = '/home/opc/downloads'
CONFIG_PATH = "/home/opc/.oci/config"