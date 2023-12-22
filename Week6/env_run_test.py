import subprocess
from time import sleep
import random
import sys

N = int(sys.argv[1]) # Number of nodes to run
# List of Python files to run
python_files = ["storage-node.py" for _ in range(N)] + ["rest-server.py"]

# Activate virtual environment
activate_env = r"C:\Users\m-esp\Documents\DistributedStorage\env\Scripts\activate.bat "  # Replace with the path to your virtualenv activation script

# Run each Python script with its corresponding arguments
processes_storage_nodes = []
processes_rest_server = None
i = 1
for script_file in python_files:
    if i <= N:
        print("Running ", script_file, " with args ", f"node0{i}", f"{i}")
        process = subprocess.Popen(
            [activate_env, "&&", "python", script_file] + [f"node{i}", f"{i}"]
        )
        print("Process ID: ", process.pid)
        processes_storage_nodes.append(process)
        i += 1
    else:
        print("Running ", script_file)
        process = subprocess.Popen(
            [activate_env, "&&", "python", script_file] + [f" {N}"]
        )
        print("Process ID: ", process.pid)
        processes_rest_server = process
    sleep(0.2)


#After some time, kill a random storage node
sleep(5)
random_node = random.randint(0, N-1)
number_of_nodes_to_kill = 2
random_nodes = random.sample(processes_storage_nodes, k = number_of_nodes_to_kill)
print("Killings node ", random_nodes + 1)
for random_node in random_nodes:
    random_node.terminate()
    random_node.wait()

#on keyboard interrupt, kill all processes
try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    for process in processes_storage_nodes:
        process.terminate()
    processes_rest_server.terminate()
    print("All processes terminated")


# # Set the delay time and number of processes to terminate
# delay_time_seconds = 5  # Set the delay time in seconds
# num_processes_to_terminate = 1  # Set the number of processes to terminate randomly

# # Wait for the specified delay time
# sleep(delay_time_seconds)

# # Get the PIDs of the processes
# pids = [str(process.pid) for process in processes_storage_nodes]

# # Start the subprocess to terminate processes after the set time
# #terminate_process = subprocess.Popen(["python", "terminate_processes.py", str(num_processes_to_terminate), str(delay_time_seconds)]+pids)

# processes_to_kill = random.sample(processes_storage_nodes, num_processes_to_terminate)
# print("processes_to_kill: ", [process.pid for process in processes_to_kill])

# # Terminating processes
# for i, process in enumerate(processes_storage_nodes):
#     if process in processes_to_kill:
#         process.terminate()
#         print(f"Terminated process {pids[i]}")

# # Wait for all subprocesses to finish
# for process in processes_storage_nodes:
#     process.wait()
#     print(f"Process {process.pid} finished")

# processes_rest_server.wait()
# print(f"Process {processes_rest_server.pid} finished")