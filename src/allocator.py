# -*- coding: utf-8 -*-
import numpy as np

from mpi4py import MPI

"""
1 - malloc
2 - get
3 - set
4 - delete
"""

class DistributedMemory:
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

    def close(self):
        self.comm.send((0, ), dest = 1)

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
                availables.append((rank + 2, start, remaining))
                start += remaining
        return availables
    
    def malloc(self, size):
        if sum(self.slave_size) < size:
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

    def speak(self, req, verbose):
        if verbose:
            if req[0] == 0:
                print("Master closing... Bye bye")
            elif req[0] == 1:
                print("Master: malloc of size {}".format(req[1]))
            else:
                print("Slave {}: Unknown Request".format(self.rank))
    
    def close_all(self):
        for rank in range(2, self.comm.Get_size()):
            self.comm.send((0, ), dest=rank)


    def run(self, verbose):
        while True:
            req = self.comm.recv(source=0)
            self.speak(req, verbose)
            if req[0] == 0:
               self.close_all()
               break
            if req[0] == 1:
                key = self.malloc(req[1])
                self.comm.send(key, dest=0)



class Slave:
    def __init__(self, rank, max_size):
        self.comm = MPI.COMM_WORLD
        self.rank = rank
        self.max_size = max_size
        self.memory = {}

    def malloc(self, key, size):
       self.memory[key] = [None] * size 

    def speak(self, req, verbose):
        if verbose:
            if req[0] == 0:
                print("Slave {} closing".format(self.rank))
            elif req[0] == 1:
                print("Slave {}: malloc of size {} for key {}".format(self.rank,
                                                                      req[2],
                                                                      req[1]))

    def run(self, verbose):
        while True:
            req = self.comm.recv(source=1)
            self.speak(req, verbose)
            if req[0] == 0:
                break
            if req[0] == 1:
                self.malloc(req[1], req[2])
            

def launch(max_size=None, verbose=False):
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return DistributedMemory(verbose)
    elif rank == 1:
        Master(max_size).run(verbose)
    else:
        Slave(rank, max_size).run(verbose)
    exit(0)
