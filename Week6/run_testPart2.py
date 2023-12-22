import subprocess
from time import sleep
import random
import sys

N = int(sys.argv[1]) # Number of nodes to run

# List of Python files to run
python_files = ["storage-node.py" for _ in range(N)] + [ "rest-server.py"]

# Run each Python script with its corresponding arguments
processes_storage_nodes = []
processes_rest_server = None
i = 1
for script_file in python_files:
    if i <= N:
        print("Running ", script_file, " with args ", f"node0{i}", f"{i}")
        process = subprocess.Popen(["python", script_file] + [f"node{i}", f"{i}"])
        print("Process ID: ", process.pid)
        processes_storage_nodes.append(process)
        i += 1
    else:
        print("Running ", script_file)
        process = subprocess.Popen(["python", script_file] + [f" {N}"])
        print("Process ID: ", process.pid)
        processes_rest_server = process
    sleep(0.2)


#After some time, kill a random storage node
sleep(5)
random_node = random.randint(0, N-1)
print("Killing node ", random_node + 1)
processes_storage_nodes[random_node].kill()


#on keyboard interrupt, kill all processes
try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    for process in processes_storage_nodes:
        process.kill()
    processes_rest_server.kill()
    print("All processes killed")
