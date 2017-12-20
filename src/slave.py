from mpi4py import MPI

class Slave:
    def __init__(self, rank, max_size):
        self.comm = MPI.COMM_WORLD
        self.rank = rank - 2
        self.max_size = max_size
        self.memory = {}

    def malloc(self, key, size):
       self.memory[key] = [None] * size 

    def getitem(self, query):
        key, start, stop, step = query
        return (key, self.memory[key][start:stop:step])

    def setitem(self, query, value):
        key, start, stop, step = query
        self.memory[key][start:stop:step] = value

    def delitem(self, key):
        del self.memory[key]

    def speak(self, req, verbose):
        if verbose >= 3:
            if req[0] == 0:
                print("Slave {}:\tclosing".format(self.rank))
        if verbose >= 2:
            if req[0] == 1:
                print("Slave {}:\tmalloc of size {} for key {}".format(self.rank,
                                                                      req[2],
                                                                      req[1]))
            elif req[0] == 2:
                print("Slave {}:\tget item {}".format(self.rank, req[1]))
            elif req[0] == 3:
                print("Slave {}:\tset item {} to {}".format(self.rank, req[1], req[2]))
            elif req[0] == 4:
                print("Slave {}:\tdel item {}".format(self.rank, req[1]))

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
            elif req[0] == 3:
                self.setitem(req[1], req[2])
            elif req[0] == 4:
                self.delitem(req[1])


