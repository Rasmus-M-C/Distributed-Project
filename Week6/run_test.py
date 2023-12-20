import subprocess
from time import sleep
import random

N = 26

number_of_nodes = 3
# List of Python files to run
python_files = ["storage-node.py" for _ in range(N)] + [ "rest-server.py"]

# Run each Python script with its corresponding arguments
processes = []

i = 1
for script_file in python_files:
    if i <= N:
        print("Running ", script_file, " with args ", f"node0{i}", f"{i}")
        process = subprocess.Popen(["python", script_file] + [f"node{i}", f"{i}"])
        i += 1
    else:
        print("Running ", script_file, " with args ", f"{N}")
        process = subprocess.Popen(["python3", script_file] + [f"{N}"])

    processes.append(process)
    sleep(0.2)
    
# Set the delay time and number of processes to terminate
delay_time_seconds = 5  # Set the delay time in seconds
num_processes_to_terminate = 2  # Set the number of processes to terminate randomly

# Get the PIDs of the processes
pids = [str(process.pid) for process in processes]

# Start the subprocess to terminate processes after the set time
#terminate_process = subprocess.Popen(["python", "terminate_processes.py", str(num_processes_to_terminate), str(delay_time_seconds)]+pids)

processes_to_kill = random.sample(processes, num_processes_to_terminate)

    
# Wait for all subprocesses to finish
for i, process in enumerate(processes):
    if process in processes_to_kill:
        sleep(5)
        process.terminate()
        sleep(0.2)
        print(f"Terminated process {pids[i]}")
    else:
        process.wait()
