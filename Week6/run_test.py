import subprocess
from time import sleep

N = 26

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
    
    

# Wait for all subprocesses to finish
for process in processes:
    process.wait()
