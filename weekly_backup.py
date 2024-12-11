import os
import subprocess
import schedule
import time
from datetime import datetime

# Database configuration
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),  # Database host address from environment variable
    "port": os.getenv("DB_PORT", "5432"),  # Port where PostgreSQL is running from environment variable
    "user": os.getenv("DB_USER", "postgres"),  # Database username from environment variable
    "password": os.getenv("DB_PASSWORD", "postgres"),  # User password from environment variable
    "database": os.getenv("DB_NAME", "projectpath_pro"),  # Name of the database to back up from environment variable
    "backup_dir": os.getenv("BACKUP_DIR", "/var/backups/postgresql/")  # Directory where backups will be saved from environment variable
}

def perform_backup():
    print("Starting backup process...")  # Log for backup initiation

    # Create the backup directory if it does not exist
    if not os.path.exists(db_config["backup_dir"]):
        os.makedirs(db_config["backup_dir"])
        print(f"Backup directory created: {db_config['backup_dir']}")

    # Create the backup file name with the current date and time
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = os.path.join(db_config["backup_dir"], f"backup_{current_date}.sql")
    print(f"Generated backup file: {backup_file}")

    # Command to perform the backup using pg_dump
    command = [
        "pg_dump",
        "--host", db_config["host"],  # Specify the host
        "--port", db_config["port"],  # Specify the port
        "--username", db_config["user"],  # Specify the user
        "--no-password",  # Prevent password prompt in the console
        "--format", "c",  # Backup format ("c" for custom)
        "--file", backup_file,  # Output backup file
        db_config["database"]  # Name of the database
    ]

    # Set the environment variable for the password
    os.environ["PGPASSWORD"] = db_config["password"]
    print("Environment variable for password set.")

    try:
        # Execute the command to create the backup
        print("Executing backup command...")
        subprocess.run(command, check=True)
        print(f"Backup completed successfully: {backup_file}")
    except subprocess.CalledProcessError as e:
        # Handle errors in case the command fails
        print(f"Error during backup: {e}")
    finally:
        # Clean up the environment variable for security
        del os.environ["PGPASSWORD"]
        print("Environment variable for password removed.")

# Schedule the backup to run once a week
schedule.every().week.do(perform_backup)
print("Task scheduled: Weekly backup.")

print("Backup service running. Press Ctrl+C to stop.")

# Keep the script running to check for scheduled tasks
while True:
    print("Checking for pending tasks...")  # Log for debugging each loop
    schedule.run_pending()  # Check and run pending tasks
    time.sleep(1)  # Wait one second before checking again
