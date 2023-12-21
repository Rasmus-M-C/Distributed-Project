import subprocess
from time import sleep
import random
import sys

N = int(sys.argv[1]) # Number of nodes to run

number_of_nodes = 3
# List of Python files to run
python_files = ["storage-node.py" for _ in range(N)] + [ "rest-server.py"]

# Run each Python script with its corresponding arguments
processes_storage_nodes = []
processes_rest_server = None
i = 1
for script_file in python_files:
    if i <= N:
        print("Running ", script_file, " with args ", f"node0{i}", f"{i}")
        process = subprocess.Popen(
            ["python", script_file] + [f"node{i}", f"{i}"],
            shell=True
        )
        print("Process ID: ", process.pid)
        processes_storage_nodes.append(process)
        i += 1
    else:
        print("Running ", script_file)
        process = subprocess.Popen(
            ["python", script_file] + [f" {N}"],
            shell=True
        )
        print("Process ID: ", process.pid)
        processes_rest_server = process
    sleep(0.2)
    
# Set the delay time and number of processes to terminate
delay_time_seconds = 5  # Set the delay time in seconds
num_processes_to_terminate = 2  # Set the number of processes to terminate randomly

# Wait for the specified delay time
sleep(delay_time_seconds)

# Get the PIDs of the processes
pids = [str(process.pid) for process in processes_storage_nodes]

# Start the subprocess to terminate processes after the set time
#terminate_process = subprocess.Popen(["python", "terminate_processes.py", str(num_processes_to_terminate), str(delay_time_seconds)]+pids)





# Wait for all subprocesses to finish
for process in processes_storage_nodes:
    process.wait()
    print(f"Process {process.pid} finished")

processes_rest_server.wait()
print(f"Process {processes_rest_server.pid} finished")
