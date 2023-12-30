import time
import messages_pb2
import math
import random
import zmq

from utils import random_string
import zmq
import json



context = zmq.Context()



def store_file(file_data, response_socket, N):
    N = int(N)
    DATAADDRESSES = [f"tcp://*:{(i+1) * 3 + 5557}" for i in range(N)]

    #A list of lists of length 3 created from N elements
    BUDDYGROUPS = [DATAADDRESSES[i:i+3] if i+3 <= N else DATAADDRESSES[i:N] for i in range(0, N, 3)]

    print("DATAADDRESSES: ", DATAADDRESSES)
    print("BUDDYGROUPS: ", BUDDYGROUPS)

    """
    Implements storing a file with RAID 1 using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param send_task_sockets: A list of ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    size = len(file_data)

    # RAID 1: cut the file in half and store both halves 2x
    file_data_1 = file_data[:math.ceil(size/4.0)]
    file_data_2 = file_data[math.ceil(1*size/4.0):math.ceil(2*size/4.0)]
    file_data_3 = file_data[math.ceil(2*size/4.0):math.ceil(3*size/4.0)]
    file_data_4 = file_data[math.ceil(3*size/4.0):math.ceil(4*size/4.0)]

    # Generate two random chunk names for each half    -- k chunks = [0, 1, 2, ..., k-1]]
    k = 3
    file_data_1_names = [random_string(8) for _ in range(k)]
    file_data_2_names = [random_string(8) for _ in range(k)]
    file_data_3_names = [random_string(8) for _ in range(k)]
    file_data_4_names = [random_string(8) for _ in range(k)]

    # Send 2 'store data' Protobuf requests with the first half and chunk names
    placementMethod = "buddygroup"

    if placementMethod == "random":
        #Random
        addresses = random.sample(DATAADDRESSES, k=N)

        for name in file_data_1_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            address = random.choice(addresses)
            send_task_socket = context.socket(zmq.PUSH)
            send_task_socket.bind(address)
            send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_1
                ])
            send_task_socket.close()
        for name in file_data_2_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            address = random.choice(addresses)
            send_task_socket = context.socket(zmq.PUSH)
            send_task_socket.bind(address)
            send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_2
                ])
            send_task_socket.close()
        for name in file_data_3_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            address = random.choice(addresses)
            send_task_socket = context.socket(zmq.PUSH)
            send_task_socket.bind(address)
            send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_3
                ])
            send_task_socket.close()
        for name in file_data_4_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            address = random.choice(addresses)
            send_task_socket = context.socket(zmq.PUSH)
            send_task_socket.bind(address)
            send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_4
                ])
            send_task_socket.close()
        # Wait until we receive 8 responses from the workers
        for task_nbr in range(len(file_data_1_names) + len(file_data_2_names) + len(file_data_3_names) + len(file_data_4_names)):
            resp = response_socket.recv_string()
            print('Received: %s' % resp)

    elif placementMethod == "minset":
        #MinSet
        addresses = random.sample(DATAADDRESSES, k=3)

        for address in addresses:
            send_task_socket = context.socket(zmq.PUSH)
            send_task_socket.bind(address)
            print("Sending to %s" % address)

            for name in file_data_1_names:
                task = messages_pb2.storedata_request()
                task.filename = name
                send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_1
                ])
            for name in file_data_2_names:
                task = messages_pb2.storedata_request()
                task.filename = name
                send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_2
                ])
            for name in file_data_3_names:
                task = messages_pb2.storedata_request()
                task.filename = name
                send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_3
                ])
            for name in file_data_4_names:
                task = messages_pb2.storedata_request()
                task.filename = name
                send_task_socket.send_multipart([
                    task.SerializeToString(),
                    file_data_4
                ])
            send_task_socket.close()
        # Wait until we receive 16 responses from the workers
        for task_nbr in range((len(file_data_1_names) + len(file_data_2_names) + len(file_data_3_names) + len(file_data_4_names)) * 3): # N = 3
            resp = response_socket.recv_string()
            print('Received: %s' % resp)

    elif placementMethod == "buddygroup":
        #BuddyGroup
        addresses = random.choice(BUDDYGROUPS)
        def send_data(file_names, file_data, addresses):
            for address in addresses:
                with context.socket(zmq.PUSH) as send_task_socket:
                    max_retries = 5
                    retry_delay = 0.1  # Start with 100ms
                    for attempt in range(max_retries):
                        try:
                            send_task_socket.bind(address)
                            for name in file_names:
                                task = messages_pb2.storedata_request()
                                task.filename = name
                                send_task_socket.send_multipart([
                                    task.SerializeToString(),
                                    file_data
                                ])
                            send_task_socket.close()
                            break  # Success, exit retry loop
                        except zmq.ZMQError as e:
                            print(f"Attempt {attempt + 1} failed, error: {e}")
                            time.sleep(retry_delay)
                            retry_delay *= 1.2
        
        print("addresses: ", addresses)

        send_data(file_data_1_names, file_data_1, addresses)
        send_data(file_data_2_names, file_data_2, addresses)
        send_data(file_data_3_names, file_data_3, addresses)
        send_data(file_data_4_names, file_data_4, addresses)

        # Wait until we receive 8 responses from the workers
        #print("Waiting for responses...", range(len(file_data_1_names) + len(file_data_2_names) + len(file_data_3_names) + len(file_data_4_names)))
        for task_nbr in range(len(file_data_1_names) + len(file_data_2_names) + len(file_data_3_names) + len(file_data_4_names)):
            resp = response_socket.recv_string()
            print('Received: %s' % resp)
        
    
    # Return the chunk names of each replica
    return file_data_1_names, file_data_2_names, file_data_3_names, file_data_4_names
#

def get_file(part1_filenames, part2_filenames, part3_filenames, part4_filenames, data_req_socket, response_socket):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param part1_filenames: List of chunk names that store the first half
    :param part2_filenames: List of chunk names that store the second half
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The original file contents
    """
    # Select one chunk of each half
    print(len(part1_filenames), len(part2_filenames), len(part3_filenames), len(part4_filenames))
    part1_filename = part1_filenames[random.randint(0, len(part1_filenames)-1)]
    part2_filename = part2_filenames[random.randint(0, len(part2_filenames)-1)]
    part3_filename = part3_filenames[random.randint(0, len(part3_filenames)-1)]
    part4_filename = part4_filenames[random.randint(0, len(part4_filenames)-1)]
    # Request both chunks in parallel
    task1 = messages_pb2.getdata_request()
    task1.filename = part1_filename
    data_req_socket.send(
        task1.SerializeToString()
    )
    task2 = messages_pb2.getdata_request()
    task2.filename = part2_filename
    data_req_socket.send(
        task2.SerializeToString()
    )
    task3 = messages_pb2.getdata_request()
    task3.filename = part3_filename
    data_req_socket.send(
        task3.SerializeToString()
    )
    task4 = messages_pb2.getdata_request()
    task4.filename = part4_filename
    data_req_socket.send(
        task4.SerializeToString()
    )

    # Receive both chunks and insert them to 
    file_data_parts = [None, None, None, None]
    while(file_data_parts.count(None) > 0):
        result = response_socket.recv_multipart()
        # First frame: file name (string)
        #print("result: %s" % result)
        filename_received = result[0].decode('utf-8')
        # Second frame: data
        print("Received %s" % filename_received)
        chunk_data = result[1]

        if filename_received == part1_filename:
            # The first part was received
            file_data_parts[0] = chunk_data
        elif filename_received == part2_filename:    
            # The second part was received
            file_data_parts[1] = chunk_data
        elif filename_received == part3_filename:
            # The third part was received
            file_data_parts[2] = chunk_data
        elif filename_received == part4_filename:
            file_data_parts[3] = chunk_data

    print("All chunks received successfully")
    
    # Combine the parts and return
    file_data = file_data_parts[0] + file_data_parts[1] + file_data_parts[2] + file_data_parts[3]
    return file_data

def fileslost(files, data_req_socket, response_socket):
    lost_files = []
    total_files = len(files)  # Count the total number of files

    for file_record in files:
        file_lost = False
        storage_mode = file_record['storage_mode']
        storage_details = json.loads(file_record['storage_details'])

        if storage_mode != 'raid1':
            continue

        for part_key in ['part1_filenames', 'part2_filenames', 'part3_filenames', 'part4_filenames']:
            if part_key not in storage_details or not storage_details[part_key]:
                continue

            part_filenames = storage_details[part_key]
            part_available = False
            for chunk_name in part_filenames:
                if check_chunk_availability(chunk_name, data_req_socket, response_socket):
                    part_available = True
                    break
            
            if not part_available:
                file_lost = True
                break

        if file_lost:
            lost_files.append(file_record['filename'])

    return lost_files, total_files
def check_chunk_availability(chunk_name, data_req_socket, response_socket):
    # Create and send a check request
    check_request_filename = "check:" + chunk_name
    task = messages_pb2.getdata_request()
    task.filename = check_request_filename
    data_req_socket.send(task.SerializeToString())

    # Poll the response socket for a reply with a timeout
    poller = zmq.Poller()
    poller.register(response_socket, zmq.POLLIN)
    poll_timeout = 5000  # Timeout in milliseconds

    try:
        if poller.poll(poll_timeout):
            # If we get a reply, check if the chunk is available
            response = response_socket.recv_string()
            return response == "Exists"
        else:
            # Timeout occurred, assume chunk is not available
            return False
    except zmq.ZMQError as e:
        print(f"Error in checking chunk availability: {e}")
        return False
