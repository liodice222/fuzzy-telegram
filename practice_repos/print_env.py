import os

def print_environment_variable(var_name):
    value = os.environ.get(var_name)
    if value is not None:
        print(f"{var_name}: {value}")
    else:
        print(f"{var_name} is not set.")

def main():
    # List of environment variables to print
    env_vars = [
        'PATH',
        'DB_USERNAME',
        'DB_PASSWORD',
        'DB_DSN',
        'TNS_ADMIN'
    ]

    # Print each environment variable
    for var in env_vars:
        print_environment_variable(var)

if __name__ == "__main__":
    main()
