from mpi4py import MPI

class Slave:
    def __init__(self, rank, max_size):
        self.comm = MPI.COMM_WORLD
        self.rank = rank - 2
        self.max_size = max_size
        self.memory = {}

    def malloc(self, key, size):
        """
        Allocates array with requested size and key

        Params:
            :key  -- int: array key (id) 
            :size -- int: size of memory that needs to be allocated
        """

        self.memory[key] = [None] * size 

    def getitem(self, query):
        """
        Returns requested slice of requested array

        Params:
            :query -- [int, int, int, int]: [key, start, stop, step]

        Return:
            :array -- [int]: requested slice of requested array
        """

        key, start, stop, step = query
        return (key, self.memory[key][start:stop:step])

    def setitem(self, query, value):
        """
        Sets requested slice of requested array to value

        Params:
            :query -- [int, int, int, int]: [key, start, stop, step]
            :value -- [int]: array of same size of slice
        """

        key, start, stop, step = query
        self.memory[key][start:stop:step] = value

    def delitem(self, key):
        """
        Deletes requested array

        Params:
            :key -- int: key (id) of array
        """
        
        del self.memory[key]

    def speak(self, request, verbose):
        """
        Prints requested action based on verbose level

        Params:
            :request -- [(...)]:
        """
        
        if verbose >= 3:
            if request[0] == 0:
                print("Slave {}:\tclosing".format(self.rank))
        if verbose >= 2:
            if request[0] == 1:
                print("Slave {}:\tmalloc of size {} for key {}".format(self.rank,
                    request[2],
                    request[1]))
            elif request[0] == 2:
                print("Slave {}:\tget item {}".format(self.rank, request[1]))
            elif request[0] == 3:
                print("Slave {}:\tset item {}".format(self.rank, request[1]))
            elif request[0] == 4:
                print("Slave {}:\tdel item {}".format(self.rank, request[1]))

    def run(self, verbose):
        """
        Slave's main loop

        Params:
            :verbose -- int: level of verbose
        """
 
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


