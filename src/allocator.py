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
    if (type(val) == int):
        return [val]
    elif (type(val) == slice):
        step = 1 if (val.step is None) else val.step
        return [i in range(val.start, val.stop, step)]


class Manager:
    def __init__(self, verbose):
        self.comm = MPI.COMM_WORLD
        self.verbose = verbose

    def handle_errors(self, response):
        if (type(response[1]) == int and response[1] < 0):
            self.close()
            if (response[1] == -1):
                raise Exception("Not enough memory")
            elif (response[1] == -2):
                raise Exception("Unknown key")
            else:
                raise Exception("Invalid request")


    def malloc(self, size):
        self.comm.send((1, size), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)
        return response[1]

    def parse_key(self, key):
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
        message  = self.parse_key(key)
        self.comm.send((2, message), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)
        return response[1]

    def __setitem__(self, key, value):
        message = self.parse_key(key)
        self.comm.send((3, message, value), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)

    def __delitem__(self, key):
        if (type(key) == tuple):
            self.handle_errors((4, -3))

        message  = self.parse_key(key)
        self.comm.send((4, message), dest=1)
        response = self.comm.recv(source=1)
        self.handle_errors(response)

    def close(self):
        self.comm.send((0, ), dest = 1)

def launch(max_size=None, verbose=0):
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return Manager(verbose)
    elif rank == 1:
        Master(max_size).run(verbose)
    else:
        Slave(rank, max_size).run(verbose)
    exit(0)
