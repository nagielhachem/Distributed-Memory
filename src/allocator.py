# -*- coding: utf-8 -*-

from mpi4py import MPI

from master import Master
from slave import Slave

"""
1 - malloc
2 - get
3 - set
4 - delete
"""


def key_to_list(val):
    """
    Transforms key to list

    Params:
        :val  -- int or slice: key to be transformed to list

    Return:
        :list -- []: transformed list
    """

    if (type(val) == int):
        return [val]
    elif (type(val) == slice):
        step = 1 if (val.step is None) else val.step
        return [i in range(val.start, val.stop, step)]


class Manager:
    def __init__(self):
        self.comm = MPI.COMM_WORLD

    def handle_errors(self, response):
        """
        Handles response error.

        Params:
            :response -- int: response status
        """
        
        if (type(response[1]) == int and response[1] < 0):
            self.close()
            if (response[1] == -1):
                raise Exception("Not enough memory")
            elif (response[1] == -2):
                raise Exception("Unknown key")
            else:
                raise Exception("Invalid request")


    def malloc(self, size):
        """
        Allocates memory with requested size
            Send message to master in order to allocate memory

        Params:
            :size -- int: size of memory that needs to be allocated

        Return:
            :key  -- int: key identifying the array to be allocated 
        """
        
        self.comm.send((1, size), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)
        return response[1]

    def parse_key(self, key):
        """
        Parses key into message
            key can be an integer representing an array key
            key can be a slice representing a list of arrays keys
            key can be a tuple 
                first value is the array key
                second value is the array slice

        Params:
            :key -- int or tuple or slice: key to parse

        Return:
            :message -- [ [int, int, int, int] ]: [ [key, start, stop, step] ]
        """
        
        message = []
        # case: request one or multiple arrays
        if (type(key) != tuple):
            for i in key_to_list(key):
                message.append([i, 0, -1, 1])
        # case: request one or multiple slices of arrays
        elif (type(key) == tuple):
            arrays = key_to_list(key[0])
            val    = key[1]
            start  = val     if (type(val) == int) else val.start
            stop   = val + 1 if (type(val) == int) else val.stop
            step   =       1 if (type(val) == int or val.step is None) else val.step
            for i in arrays:
                message.append([i, start, stop, step])
        return message

    def __getitem__(self, key):
        """
        Gets values of items on requested key.
            Parse key
            Send request to Master
            Wait for response from Master
            Handle error
            Return result

        Params:
            :key    -- int or tuple or slice: requested key

        Return:
            :result -- [ [int] ]
        """
        
        message  = self.parse_key(key)
        self.comm.send((2, message), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)
        return response[1]

    def __setitem__(self, key, value):
        """
        Sets requested items to value.
            Parse key
            Send request to Master
            Wait for response from Master
            Handle error

        Params:
            :key   -- int or tuple: requested key
            :value -- int or [int]: value to be set
        """
        
        message = self.parse_key(key)
        self.comm.send((3, message, value), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)

    def __delitem__(self, key):
        """
        Deletes array with requested key
            Parse key
            Send request to Master
            Wait for response from Master
            Handle error

        Params:
            :key -- int or slice: key (id) of the array to be deleted
        """
        
        if (type(key) == tuple):
            self.handle_errors((4, -3))

        message  = self.parse_key(key)
        self.comm.send((4, message), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)

    def close(self):
        self.comm.send((0, ), dest = 1)

def launch(max_size=None, verbose=0):
    """
    Launch all machines

    Params:
        :max_size -- int: max_size of each machine
        :verbose  -- int: level of verbose

    Return:
        :manager  -- Manager: an instance of the memory manager 
    """
    
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return Manager()
    elif rank == 1:
        Master(max_size).run(verbose)
    else:
        Slave(rank, max_size).run(verbose)
    exit(0)
