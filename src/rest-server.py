"""
Aarhus University - Distributed Storage course - Lab 6

REST API + RAID Controller
"""
from flask import Flask, make_response, g, request, send_file
import sqlite3
import base64
import random
import string
import logging

import zmq # For ZMQ
import time # For waiting a second for ZMQ connections
import math # For cutting the file in half
import messages_pb2 # Generated Protobuf messages
import io # For sending binary data in a HTTP response
import logging

import raid1

from utils import is_raspberry_pi
import sys

import threading


N = sys.argv[1] if len(sys.argv) > 1 else 9

print("N: ",N)

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            'files.db',
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


# Initiate ZMQ sockets
context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5556") ## skal ændres

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558") ## skal ændres

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")


# Wait for all workers to start and connect. 
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5560")



#look for heartbeats on "tcp://localhost:5555"
heartbeat_socket = context.socket(zmq.PULL)
heartbeat_socket.bind("tcp://*:5555")
#heartbeat_socket.setsockopt(zmq.SUBSCRIBE, b'')

shutdown_event = threading.Event()

#add a way to keep track of storage_nodes
storage_nodes = {}


#receive heartbeats from all workers on a separate thread

def receive_heartbeats():
    while not shutdown_event.is_set():
        try:
            msg = heartbeat_socket.recv_string(flags=zmq.NOBLOCK)
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                # print("Waiting for heartbeat...")
                #print("Waiting for heartbeat...")
                pass  # no message was ready
            else:
                raise  # real error
        else:
            # process message
            # print(f"{msg}")
            # add storage node to list of storage nodes
            storage_nodes[msg] = time.time()
            #print(f"Storage nodes: {storage_nodes}")

        # check if any storage nodes have not sent a heartbeat in the last 20 seconds
        for node in list(storage_nodes.keys()):
            if time.time() - storage_nodes[node] > 20:
                print(f"Storage node {node} is dead")
                del storage_nodes[node]

        time.sleep(.1)

heartbeat_thread = threading.Thread(target=receive_heartbeats)
heartbeat_thread.start()

#close thread function to be called on shutdown
def close_heartbeats():
    shutdown_event.set()
    heartbeat_thread.join()
    print("Heartbeat thread closed")

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
#app.teardown_appcontext([close_db, close_heartbeats])

@app.teardown_appcontext
def teardown_app_context(error=None):
    close_db()
    close_heartbeats()

@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})

@app.route('/files',  methods=['GET'])
def list_files():
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to 
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]
    
    return make_response({"files": files})
#

@app.route('/files/<int:file_id>',  methods=['GET'])
def download_file(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f['filename']))
    
    # Parse the storage details JSON string
    import json
    storage_details = json.loads(f['storage_details'])

    if f['storage_mode'] == 'raid1':
        part1_filenames = storage_details['part1_filenames'] 
        part2_filenames = storage_details['part2_filenames']
        part3_filenames = storage_details['part3_filenames']
        part4_filenames = storage_details['part4_filenames']
        
        file_data = raid1.get_file(
            part1_filenames,
            part2_filenames,
            part3_filenames,
            part4_filenames,
            data_req_socket,
            response_socket
        )

    elif f['storage_mode'] == 'eras
    ure_coding_rs':
        # TODO Handle Reed Solomon
        # dummyImage to simulate content retrieval
        with open("dummyImage.png", "rb") as image:
            file = image.read()
            file_data = bytearray(file)
        pass


    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])
#

# HTTP HEAD requests are served by the GET endpoint of the same URL,
# so we'll introduce a new endpoint URL for requesting file metadata.
@app.route('/files/<int:file_id>/info',  methods=['GET'])
def get_file_metadata(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File: %s" % f)

    return make_response(f)
#

@app.route('/files/<int:file_id>',  methods=['DELETE'])
def delete_file(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File to delete: %s" % f)

    # TODO Delete all chunks from the Storage Nodes

    # TODO Delete the file record from the DB

    # Return empty 200 Ok response
    return make_response('TODO: implement this endpoint', 404)
#


@app.route('/files', methods=['POST'])
def add_files():
    payload = request.get_json()
    filename = payload.get('filename')
    content_type = payload.get('content_type')
    file_data = base64.b64decode(payload.get('contents_b64'))
    size = len(file_data)

    # -- should receive 4 names, the lenth of each list is the number of replicas k
    file_data_1_names, file_data_2_names = raid1.store_file(file_data, send_task_socket, response_socket)
    
    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `part1_filenames`, `part2_filenames`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, ','.join(file_data_1_names), ','.join(file_data_2_names))
    )
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid }, 201)
#

@app.route('/files_mp', methods=['POST'])
def add_files_multipart():
    # Flask separates files from the other form fields
    payload = request.form
    files = request.files

    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)
    
    # Reference to the uploaded file under the 'file' key
    file = files.get('file')
    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = file.read()
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))
    
    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'raid1')
    print("Storage mode: %s" % storage_mode)

    if storage_mode == 'raid1':
        file_data_1_names, file_data_2_names, file_data_3_names, file_data_4_names = raid1.store_file(data, response_socket, N)
        
        storage_details = {
            "part1_filenames": file_data_1_names,
            "part2_filenames": file_data_2_names,
            "part3_filenames": file_data_3_names,
            "part4_filenames": file_data_4_names
        }
   
    elif storage_mode == 'erasure_coding_rs':
        # TODO implement Reed Solomon code
        # Parse max_erasures (everything is a string in request.form,
        # we need to convert to int manually), set default value to 1
        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))
        #TODO Implement erasure coding
        storage_details = {
            "part1_filenames": "",
            "part2_filenames": "",
        }
        pass

    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)
    
    # Insert the File record in the DB
    import json
    db = get_db()
    cursor = db.execute("INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`,`storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()


    return make_response({"id": cursor.lastrowid }, 201)
#
@app.route('/lost',  methods=['GET'])
def quantify_lost_files():
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    files = cursor.fetchall()
    files = [dict(file) for file in files]

    lost_files, total_files = raid1.fileslost(files, data_req_socket, response_socket)
    fraction_lost = len(lost_files) / total_files if total_files > 0 else 0

    return make_response({
        "total_files": total_files,
        "lost_files_count": len(lost_files),
        "fraction_lost": fraction_lost,
        "lost_files": lost_files
    })
#\
@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)



# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host= host_local_computer, port=9005)