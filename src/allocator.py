# -*- coding: utf-8 -*-
import numpy as np

from mpi4py import MPI

"""
1 - malloc
2 - get
3 - set
4 - delete
"""

class API:
    def __init__(self, verbose):
        self.comm = MPI.COMM_WORLD
        self.verbose = verbose

    def malloc(self, size):
        self.comm.send((1, size), dest=1)
        key = self.comm.recv(source=1)
        return key

    def __getitem__(self, key):
        self.comm.send(key, dest=1)
        value = self.comm.recv(source=1)
        return value

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

class Master:
    def __init__(self, max_size):
        self.comm = MPI.COMM_WORLD
        self.max_size = max_size
        self.counter = 0
        self.block_infos = {}
        self.slave_size = [max_size] * (self.comm.Get_size() - 2)

    def choose_slaves(self, size):
        """
        return tableau de rank de slave pouvant stocker le tableau
        param:
            size: 
            return: [(rank, start, offset)]
        """
        nb_slaves = len(self.slave_size)
        availables = []
        start = 0
        for rank in range(nb_slaves):
            if size == start:
                break
            remaining = min(self.slave_size[rank], size - start)
            if remaining != 0:
                availables.append((rank, start, remaining))
                start += remaining
        return availables

    
    def malloc(self, size):
        if sum(self.slave_size) > size:
            return -1
        key = self.counter
        available_slaves = self.choose_slaves(size) 
        # update block_infos
        self.block_infos[key] = available_slaves 
        for rank, start, offset in available_slaves:
            self.comm.send((1, key, offset), dest=rank)
            # update slave_size
            self.slave_size[rank - 2] -= offset
        self.counter += 1
        return key

    def run(self, verbose):
        while True:
            req = self.comm.recv(source=0)
            if req[0] == 1:
                key = self.malloc(req[1])
                self.comm.send(key, dest=0)




class Slave:
    def __init__(self, max_size):
        self.comm = MPI.COMM_WORLD
        self.max_size = max_size

    def malloc(self, size):
        pass

    def run(self, verbose):
        pass

def launch(max_size=None, verbose=False):
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return API(verbose)
    elif rank == 1:
        Master(max_size).run(verbose)
    else:
        Slave(max_size).run(verbose)
