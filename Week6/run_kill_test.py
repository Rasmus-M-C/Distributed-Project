import os
import random
import sqlite3
import json
import matplotlib.pyplot as plt

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
    percentage = ((total_files - corrupt_files) / total_files)

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

#12, 24, 36
N = 36
 
s_list = [2, 3, 4, 6, 8, 10]

#Append restults to a json file

file_name = 'buddygroup_kill_results.json'
# Read existing results from the file
try:
    with open(file_name, 'r') as json_file:
        results = json.load(json_file)
except FileNotFoundError:
    # If the file doesn't exist, initialize an empty list
    results = []

#Using a for loop get the average percentage over 100 tests
for s in s_list:
    total_percentage = 0
    print(f"Running test with s = {s} and N = {N}")
    for i in range(100):
        percentage = 1 - check_file_chunks(database_path, s, N)
        total_percentage += percentage

    print(f"s:{s} loss_percentage: {total_percentage/100}")
    results.append({"s": s, "N": N, "loss_percentage": total_percentage/100})

# Write the updated results back to the file
with open(file_name, 'w') as json_file:
    json.dump(results, json_file, indent=2)











# Plotting the results

# Function to read results from a JSON file and filter by N
def read_and_filter_results(filename, target_N):
    with open(filename, 'r') as json_file:
        results = json.load(json_file)
    
    # Filter results based on the target N
    filtered_results = [result for result in results if result['N'] == target_N]

    return filtered_results

# Read and filter results from the JSON file for a specific N
input_filename = file_name
target_N_value = N  # Change this to the desired N value
filtered_results = read_and_filter_results(input_filename, target_N_value)

# Extract data for plotting from the filtered results
s_values = [result['s'] for result in filtered_results]
percentage_values = [result['loss_percentage'] for result in filtered_results]

# Create a plot
plt.plot(s_values, percentage_values, marker='o', linestyle='-', color='b')
plt.title(f'Percentage vs. s Values for N = {target_N_value}')
plt.xlabel('s')
plt.ylabel('Percentage')
plt.grid(True)
plt.show()




#buddygroup
#Running test with s = 2 and N = 12
# s:2 percentage: 91.94845360824746%
# Running test with s = 3 and N = 12
# s:3 percentage: 84.19587628865982%
# Running test with s = 4 and N = 12
# s:4 percentage: 75.35051546391753%
# Running test with s = 6 and N = 12
# s:6 percentage: 56.78350515463917%
# Running test with s = 8 and N = 12
# s:8 percentage: 34.19587628865978%
# Running test with s = 10 and N = 12
# s:10 percentage: 15.680412371134024%

#random
# Running test with s = 2 and N = 12
# s:2 percentage: 97.38383838383845%
# Running test with s = 3 and N = 12
# s:3 percentage: 93.60606060606062%
# Running test with s = 4 and N = 12
# s:4 percentage: 85.4545454545455%
# Running test with s = 6 and N = 12
# s:6 percentage: 59.33333333333339%
# Running test with s = 8 and N = 12
# s:8 percentage: 25.282828282828287%
# Running test with s = 10 and N = 12
# s:10 percentage: 3.181818181818179%
    
#minset
# Running test with s = 2 and N = 12
# s:2 percentage: 100.0%
# Running test with s = 3 and N = 12
# s:3 percentage: 99.51%
# Running test with s = 4 and N = 12
# s:4 percentage: 98.27%
# Running test with s = 6 and N = 12
# s:6 percentage: 90.85%
# Running test with s = 8 and N = 12
# s:8 percentage: 75.19%
# Running test with s = 10 and N = 12
# s:10 percentage: 45.32%
    
