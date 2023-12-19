import messages_pb2
import math
import random

from utils import random_string

def store_file(file_data, send_task_socket, response_socket):
    """
    Implements storing a file with RAID 1 using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
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
    k = 2
    file_data_1_names = [random_string(8) for _ in range(k)]
    file_data_2_names = [random_string(8) for _ in range(k)]
    file_data_3_names = [random_string(8) for _ in range(k)]
    file_data_4_names = [random_string(8) for _ in range(k)]
    print("Filenames for part 1: %s" % file_data_1_names)
    print("Filenames for part 2: %s" % file_data_2_names)
    print("Filenames for part 3: %s" % file_data_3_names)
    print("Filenames for part 4: %s" % file_data_4_names)


    # Send 2 'store data' Protobuf requests with the first half and chunk names
    for name in file_data_1_names:
        task = messages_pb2.storedata_request()
        task.filename = name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data_1
        ])

    # Send 2 'store data' Protobuf requests with the second half and chunk names
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

    # Wait until we receive 4 responses from the workers
    for task_nbr in range(4):
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
    for _ in range(4):
        result = response_socket.recv_multipart()
        # First frame: file name (string)
        print("result: %s" % result)
        filename_received = result[0].decode('utf-8')
        # Second frame: data
        print("Received %s" % filename_received)
        chunk_data = result[1]

        print("Received %s" % filename_received)

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
#