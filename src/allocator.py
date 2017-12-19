# -*- coding: utf-8 -*-
import numpy as np

from mpi4py import MPI

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


class DistributedMemory:
    def __init__(self, verbose):
        self.comm = MPI.COMM_WORLD
        self.verbose = verbose

    def malloc(self, size):
        self.comm.send((1, size), dest=1)
        key = self.comm.recv(source=1)
        return key

    def __getitem__(self, key):
        message = []
        # case: request one or multiple arrays
        if (type(key) != tuple):
            for i in range(key_to_list(key)):
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

        self.comm.send((2, message), dest=1)
        key = self.comm.recv(source=1)
        return key

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

    def size_of(self, key):
        result = 0
        for _, _, offset in self.block_infos[key]:
            result += offset
        return result

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

    def is_conform(self, requests):
        total_size = 0
        for i, request in enumerate(requests):
            key, start, stop, step = request

            if (not key in self.block_infos):
                return False

            if (stop == -1):
                stop = self.size_of(key)
            total_size += (stop - start) // step       \
                       +  bool((stop - start) % step)

        if (total_size > self.max_size):
            return False
        return True

    def split_request(self, request):
        """
            return message:
            [[rank, key, start, stop, step]]
        """
        key, start_mem, stop_mem, step_mem = request
        message = []
        cumulated_offset = 0
        for rank_block, start_block, offset_block in self.block_infos[key]:
            if start_mem < start_block + offset_block and start_mem < stop_mem:
                step_slave   = cumulated_offset % step_mem
                start_slave  = start_mem - start_block + step_slave 
                offset_slave = min(offset_block - start_slave, stop_mem - start_mem) 
                message.append([rank_block,
                                key,
                                start_slave,
                                start_slave + offset_slave - step_slave,
                                step_mem])
                # update parameters
                start_mem += offset_slave
                cumulated_offset += offset_block
        return message

    def merge_responses(self, responses):
        arrays = {}
        for rank, response in responses:
            key, array = response
            if key not in arrays:
                arrays[key] = []
            arrays[key].append((rank, array))
        results = []
        for key in sorted(arrays.keys()):
            result = []
            for response in sorted(arrays[key]):
                result += response[1]
            results.append(result)
        return results

    def getitem(self, requests):
        """
        :params:
            :requests: [key, start, stop, step]
        """
        if (not self.is_conform(requests)):
            return -1

        queries = [subrequest for request    in requests
                              for subrequest in self.split_request(request)]

        for query in queries:
            rank    = query[0]
            message = query[1:]
            self.comm.isend((2, message), dest=rank)

        responses = []
        for rank, _, _, _, _ in queries:
            responses.append((rank, self.comm.recv(source=rank)))

        return self.merge_responses(responses)


    def speak(self, req, verbose):
        if verbose:
            if req[0] == 0:
                print("Master closing... Bye bye")
            elif req[0] == 1:
                print("Master: malloc of size {}".format(req[1]))
            elif req[0] == 2:
                print("Master: get items\n{}".format(req[1]))
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
            elif req[0] == 1:
                key = self.malloc(req[1])
                self.comm.send(key, dest=0)
            elif req[0] == 2:
                val = self.getitem(req[1])
                self.comm.send(val, dest=0)



class Slave:
    def __init__(self, rank, max_size):
        self.comm = MPI.COMM_WORLD
        self.rank = rank - 2
        self.max_size = max_size
        self.memory = {}

    def malloc(self, key, size):
#       self.memory[key] = [None] * size 
       self.memory[key] = [i for i in range(size)] 

    def getitem(self, query):
        key, start, stop, step = query
        return (key, self.memory[key][start:stop:step])

    def speak(self, req, verbose):
        if verbose:
            if req[0] == 0:
                print("Slave {} closing".format(self.rank))
            elif req[0] == 1:
                print("Slave {}: malloc of size {} for key {}".format(self.rank,
                                                                      req[2],
                                                                      req[1]))
            elif req[0] == 2:
                print("Slave {}: get item {}".format(self.rank, req[1]))

    def run(self, verbose):
        while True:
            req = self.comm.recv(source=1)
            self.speak(req, verbose)
            if req[0] == 0:
                break
            elif req[0] == 1:
                self.malloc(req[1], req[2])
            elif req[0] == 2:
                val = self.getitem(req[1])
                self.comm.send(val, dest=1)


def launch(max_size=None, verbose=False):
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return DistributedMemory(verbose)
    elif rank == 1:
        Master(max_size).run(verbose)
    else:
        Slave(rank, max_size).run(verbose)
    exit(0)
