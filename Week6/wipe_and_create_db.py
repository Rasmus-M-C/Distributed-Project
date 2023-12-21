import sqlite3
import os
import shutil
import subprocess

def wipe_and_create_database():
    # Define the SQLite database file
    db_file = 'files.db'
    sql_script = 'create_table.sql'

    # Close any existing database connections (if any)
    try:
        conn = sqlite3.connect(db_file)
        conn.close()
    except sqlite3.Error as e:
        print(f"Error closing existing database connection: {e}")

    # Wipe the database file
    try:
        os.remove(db_file)
        print(f"Database file '{db_file}' wiped.")
    except OSError as e:
        print(f"Error wiping database file: {e}")

    # Create a new database and table using sqlite3.exe
    try:
        subprocess.run(['sqlite3.exe', db_file, f'.read {sql_script}'], check=True, shell=True)
        print(f"Database '{db_file}' created successfully using SQL script.")
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlite3.exe: {e}")

    # Remove files/directories starting with the prefix "node"
    try:
        for file_or_dir in os.listdir('.'):
            if file_or_dir.startswith('node'):
                full_path = os.path.join('.', file_or_dir)
                if os.path.isfile(full_path):
                    os.remove(full_path)
                    print(f"File '{full_path}' removed.")
                elif os.path.isdir(full_path):
                    shutil.rmtree(full_path)
                    print(f"Directory '{full_path}' removed.")
    except OSError as e:
        print(f"Error removing files/directories: {e}")

if __name__ == "__main__":
    wipe_and_create_database()
