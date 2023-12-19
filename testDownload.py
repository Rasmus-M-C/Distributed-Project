import requests
import time
import matplotlib.pyplot as plt
import numpy as np
import sys

# API endpoint for file upload
upload_url = "http://localhost:9000/files_mp"

# List of file names to upload
file_names = ["Data/100KB.bin", "Data/1MB.bin", "Data/10MB.bin", "Data/100MB.bin"]

# Number of times to upload each file
if len(sys.argv) > 1:
    num_uploads = int(sys.argv[1])
else:
    num_uploads = 100

# List to store all upload times
all_upload_times = []

# Define the payload (form data) for each request
payload = {
    'storage': 'raid1'
}

# Define colors for each file
colors = ['b', 'g', 'r', 'c']

# Function to calculate and plot the histogram
def plot_histogram(upload_times, file_names):
    plt.hist(upload_times, bins=20, alpha=0.5, color=colors, label=file_names)
    plt.xlabel('Upload Time (seconds)')
    plt.ylabel('Frequency')
    plt.title('Combined Upload Time Histogram')
    plt.legend()
    plt.grid(True)
    plt.show()

# Perform the uploads and record times
for file_name in file_names:
    print(f"Uploading {file_name}:")
    file_upload_times = []
    for i in range(num_uploads):
        # Prepare the file to upload
        with open(file_name, 'rb') as file:
            files = {'file': (file_name, file.read())}
        
        # Record the start time
        start_time = time.time()
        
        # Make the POST request to upload the file with form data
        response = requests.post(upload_url, data=payload, files=files)
        
        # Record the end time
        end_time = time.time()
        
        # Calculate the upload time
        upload_time = end_time - start_time
        
        # Append the upload time to the list
        file_upload_times.append(upload_time)
        
        # Print progress statement
        if (i + 1) % 5 == 0:
            print(f"Uploaded {i + 1}/{num_uploads} times for {file_name}")

    all_upload_times.append(file_upload_times)

# Calculate the mean and median upload times for all files
mean_upload_time = np.mean(np.concatenate(all_upload_times))
median_upload_time = np.median(np.concatenate(all_upload_times))
    
print("Combined Upload Times:")
print(f"Average Upload Time: {mean_upload_time:.2f} seconds")
print(f"Median Upload Time: {median_upload_time:.2f} seconds")

# Plot histogram for combined upload times with different colors
plot_histogram(all_upload_times, file_names)
