import pyerasure
import math
import random
import copy # for deepcopy
from utils import random_string
import messages_pb2

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]

def store_file(file_data, max_erasures, send_task_socket, response_socket):
    pass

def get_file(coded_fragments, max_erasures, file_size, data_req_socket, response_socket):
   pass