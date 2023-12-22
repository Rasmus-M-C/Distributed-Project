import subprocess
from time import sleep
import random
import sys

try:
    N = int(sys.argv[1]) # Number of nodes to run

    # List of Python files to run
    python_files = ["storage-node.py" for _ in range(N)] + [ "rest-server.py"]

    # Run each Python script with its corresponding arguments
    processes_storage_nodes = []
    processes_rest_server = None
    i = 1
    for script_file in python_files:
        if i <= N:
            print("Running ", script_file, "with args ",f"node0{i} ",f"{i}")
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
    random_node = random.sample(range(N), 2)
    print("Killing nodes ", [ id + 1 for id in random_node])
    for id in random_node:
        processes_storage_nodes[id].kill()


#on keyboard interrupt, kill all processes
    while True:
        sleep(1)
except KeyboardInterrupt:
    print("\nKeyboard interrupt")
    for process in processes_storage_nodes:
        process.kill()
    processes_rest_server.kill()
    print("All processes killed")
except Exception as e:
    print(e)
    for process in processes_storage_nodes:
        process.kill()
    processes_rest_server.kill()
    print("All processes killed")