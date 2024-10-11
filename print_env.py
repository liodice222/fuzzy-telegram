import os

def print_environment_variables():
    # Get all environment variables
    env_vars = os.environ

    # Print each environment variable
    for key, value in env_vars.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    print_environment_variables()
