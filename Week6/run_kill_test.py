import os
import random
import sqlite3
import json

def check_file_chunks(database_path, s, N):
    # Load the database containing file names for each chunk
    rows = load_database(database_path)

    # Get the list of folders to search
    folders = get_folders_to_search(s, N)
    #print(folders)

    # Initialize counters
    total_files = 0
    corrupt_files = 0


    # Iterate over each file in the database

#     CREATE TABLE IF NOT EXISTS `file` (
#     `id` INTEGER PRIMARY KEY AUTOINCREMENT,
#     `filename` TEXT,
#     `size` INTEGER,
#     `content_type` TEXT,
#     `storage_mode` TEXT,
#     `storage_details` TEXT,
#     `created` DATETIME DEFAULT CURRENT_TIMESTAMP
# );
    
    #read this string and cast to a dictionary and get the list in each entry
    #The string looks like tis {"part1_filenames": ["cNTPY32u", "DEEV00gJ", "sUJvBlGs"], "part2_filenames": ["USgHL4Bb", "UiPteUnS", "Rx81zSE8"], "part3_filenames": ["1U3JtBkG", "3IFos3yq", "fjt17ZLR"], "part4_filenames": ["2osF08ir", "z87bCipi", "mSGBJ3Uk"]}
    
    
    for dict in rows:
        #print(row)
        dict = json.loads(dict[0])
        #print(row['part1_filenames'])
    
        chunk_exists_in_folders = True
        for key in dict:
            chunk_exists_in_folder = False
            for chunk in dict[key]:
                if chunk_exists(chunk, folders):
                    chunk_exists_in_folder = True
                    break
            if not chunk_exists_in_folder:
                chunk_exists_in_folders = False
                break

        # Update counters
        total_files += 1
        if not chunk_exists_in_folders:
            corrupt_files += 1

    # Calculate the percentage of files with at least one copy of each chunk
    percentage = ((total_files - corrupt_files) / total_files) * 100

    return percentage

def load_database(database_path):
    # Load the database from the specified path
    # Implement your logic here
    #read the .db file and return the database



    # Connect to SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Select all rows from the users table
    cursor.execute('SELECT storage_details FROM file')

    # Fetch all rows
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    return rows
    

def get_folders_to_search(s, N):
    # Get the list of folders to search, excluding the specified folders
    # Implement your logic here
    #create a list of all folders in the current directory that start with node
    folders = [folder for folder in os.listdir() if folder.startswith("node")]
    #print(len(folders))

    #create a list of s folders to exclude, the folder number should be random witiin the range of 1 to 10
    folders_to_exclude = random.sample(folders, s)
    #print(folders_to_exclude)
    #remove the folders to exclude from the list
    for folder in folders_to_exclude:
        folders.remove(folder)

    return folders

def chunk_exists(chunk, folders):
    # Check if at least one file with the given chunk exists in the folders
    # Implement your logic here
    #iterate over each folder
    for folder in folders:
        #check if the chunk exists in the folder
        if chunk in os.listdir(folder):
            return True

# Example usage
database_path = "files.db"




s = 5
N = 6

s_list = [2, 3, 4, 6, 8, 10]

#Using a for loop get the average percentage over 100 tests
for s in s_list:
    if s >= N:
        break
    total_percentage = 0
    for i in range(1000):
        percentage = check_file_chunks(database_path, s, N)
        total_percentage += percentage

    print(f"s:{s} percentage: {total_percentage/1000}%")


# percentage = check_file_chunks(database_path, s, N)
# print(f"Percentage of files with at least one copy of each chunk: {percentage}%")
