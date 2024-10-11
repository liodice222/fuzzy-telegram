import os

def print_environment_variables():
    # Get all environment variables
    print("TNS_ADMIN:")
    tns_admin = os.environ.get(TNS_ADMIN)
    print(tns_admin, "/n")
    print("DB_USERNAME:")
    db_username = os.environ.get(DB_USERNAME)
    print(db_username, "/n")


if __name__ == "__main__":
    print_environment_variables()
