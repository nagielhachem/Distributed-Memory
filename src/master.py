from mpi4py import MPI

class Master:
    def __init__(self, max_size):
        self.comm = MPI.COMM_WORLD
        self.max_size = max_size
        self.key_generator = 0
        self.block_infos = {}
        self.slave_size = [max_size] * (self.comm.Get_size() - 2)

    def size_of(self, key):
        """
        Gets the allocated size of the array.

        Params:
            :key  -- int: key (id) of the array

        Return:
            :size -- int: size of array
        """

        result = 0
        for _, _, offset in self.block_infos[key]:
            result += offset
        return result

    def choose_slaves(self, size):
        """
        Chooses salves that will be used to allocate memory.
        
        Params:
            :size   -- int: size of memory that needs to be allocated

        Return:
            :slaves -- [(int, int, int)]: chosen slaves and info [(rank, start, offset)]
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
        """
        Send malloc message to chosen slaves. 
            malloc message format: (1, key, offset).

        Params:
            :size -- int: size of memory that needs to be allocated

        Return:
            :key  -- int: key identifying the array to be allocated 
        """

        if sum(self.slave_size) < size:
            return -1
        key = self.key_generator
        available_slaves = self.choose_slaves(size) 
        # update block_infos
        self.block_infos[key] = available_slaves 
        for rank, start, offset in available_slaves:
            self.comm.send((1, key, offset), dest=rank)
            # update slave_size
            self.slave_size[rank - 2] -= offset
        # update key generator
        self.key_generator += 1
        return key

    def is_not_conform(self, requests):
        """
        Checks if requests are conform to allocator settings.
            check if total requests size is less than total remaining slave memory.
            check if array with that key is available (has already been allocated).
            
        Params:
            :requests -- [[int, int, int, int]]: [[key, start, stop, step]]

        Return:
            :status   -- int: error message
                 0 if no error
                -1 if size is too big
                -2 if key is not conform
        """

        total_size = 0
        for i, request in enumerate(requests):
            key, start, stop, step = request

            if key not in self.block_infos:
                return -2

            if (stop == -1):
                request[2] = self.size_of(key)
            total_size += (stop - start) // step       \
                       +  bool((stop - start) % step)

        if (total_size > self.max_size):
            return -1
        return 0

    def split_request(self, request):
        """
        Split request into multiple subrequests.
        Finds which slaves contains the requested array and construct a request per slave

        Params:
            :request    -- [int, int, int, int]: [key, start, stop, step]

        Return:
            :subrequest -- [[int, int, int, int, int]]: [[rank, key, start, stop, step]]
        """

        key, start_mem, stop_mem, step_mem = request
        subrequests = []
        cumulated_offset = 0
        for rank_block, start_block, offset_block in self.block_infos[key]:
            if start_mem < start_block + offset_block and start_mem < stop_mem:
                # get slave request parameters 
                step_slave   = cumulated_offset % step_mem
                start_slave  = start_mem - start_block + step_slave 
                offset_slave = min(offset_block - start_slave, stop_mem - start_mem) 
                # append request to return array
                subrequests.append([rank_block,
                                key,
                                start_slave,
                                start_slave + offset_slave - step_slave,
                                step_mem])
                # update parameters
                start_mem += offset_slave
                cumulated_offset += offset_block
        return subrequests

    def merge_responses(self, responses):
        """
        Merge responses returned by slaves into one response. 
            Group responses by key:
                [ (rank, (key, array)) ]   --> { key: [ (rank, array) ] } 
            Order by key, Concatenate arrays with same key: 
                { key: [ (rank, array) ] } --> [ arrays concatenation ordered by key ]

        Params:
            :responses -- [(int, (int, [])] : [ (rank, (key, slave response array)) ]
    
        Return:
            :results   -- [[]] : [ arrays concatenation ordered by key ]
        """

        # group respones by key
        arrays = {}
        for rank, response in responses:
            key, array = response
            if key not in arrays:
                arrays[key] = []
            arrays[key].append((rank, array))

        # order arrays by key 
        # concatenate arrays with same key
        results = []
        for key in sorted(arrays.keys()):
            result = []
            for response in sorted(arrays[key]):
                result += response[1]
            results.append(result)
        return results

    def getitem(self, requests):
        """
        Take requests, parse them and send subrequests to concerned slaves.
            requests can be one or more array
            each array can be hole or sliced [start:stop:step]

        Params:
            :requests -- [ [int, int, int, int] ]: [ [key, start, stop, step] ]

        Return:
            :results  -- [[]] : [ requested arrays ]
        """

        status = self.is_not_conform(requests)
        if (status != 0):
            return status

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


    def setitem(self, requests, value):
        """
        Sets requested items to value
            requests can be one array or slice of arrays
            requested array can be hole or sliced [start:stop:step]
            value can be an integer which will be broadcasted int arrays
            value can be an array of the same size of the slice
            if request contains multiple arrays, they will all be set to the same value

        Params:
            :requests -- [ [int, int, int, int] ]: [ [key, start, stop, step] ]
            :value    -- int or [int]:

        Return:
            :status   -- int: status value
                 0 if no set is successful
                -3 if value and requests have not same size
        """
        status = self.is_not_conform(requests)
        if status != 0:
            return status

        _, start, stop, step = requests[0]
        total_size = (stop - start)     // step \
                   + bool((stop - start) % step)
        if type(value) == int:
            value = [value] * total_size
        else:
            if len(value) != total_size:
                return -3

        queries = [subrequest for request    in requests
                              for subrequest in self.split_request(request)]

        shift    = 0
        last_key = None
        for query in queries:
            rank, key, start, stop, step = query

            if last_key != key:
                shift    = 0
                last_key = key

            message    = query[1:]
            total_size = (stop - start) // step       \
                       +  bool((stop - start) % step)

            sub_value = value[shift: shift + total_size]
            self.comm.send((3, message, sub_value), dest=rank)

            shift += total_size
        return 0


    def delitem(self, requests):
        """
        Deletes array(s) in requests

        Params:
            :requests -- [ [int, int, int, int] ]: [ [key, _, _, _] ]

        Return:
            :status   -- int: delete status
                 0 if deletion successful
                -2 if  no array with requested key
        """
        
        status = 0
        for key, _, _, _ in requests:
            if (not key in self.block_infos):
                status = -2
                break
            for rank, _, _ in self.block_infos[key]:
                self.comm.send((4, key), dest=rank)
            del self.block_infos[key]

        return status

    def speak(self, request, verbose):
        """
        Prints requested action based on verbose level

        Params:
            :request -- [(...)]:
        """
        
        if (verbose >= 1):
            if request[0] == 0:
                print("Master:\t\tclosing")
            elif request[0] == 1:
                print("Master:\t\tmalloc of size {}".format(request[1]))
            elif request[0] == 2:
                print("Master:\t\tget items\n{}".format(request[1]))
            elif request[0] == 3:
                print("Master:\t\tset items\n{}".format(request[1]))
            elif request[0] == 4:
                print("Master:\t\tdel items\n{}".format(request[1]))
            else:
                print("Master:\t\tUnknown Request")

    def close_all(self):
        """
        Sends close message to all slaves
        """
        
        for rank in range(2, self.comm.Get_size()):
            self.comm.send((0, ), dest=rank)

    def run(self, verbose):
        """
        Master's main loop

        Params:
            :verbose -- int: level of verbose
        """
        
        while True:
            req = self.comm.recv(source=0)
            self.speak(req, verbose)
            if req[0] == 0:
               self.close_all()
               break
            elif req[0] == 1:
                key = self.malloc(req[1])
                self.comm.send((1, key), dest=0)
            elif req[0] == 2:
                val = self.getitem(req[1])
                self.comm.send((2, val), dest=0)
            elif req[0] == 3:
                val = self.setitem(req[1], req[2])
                self.comm.send((3, val), dest=0)
            elif req[0] == 4:
                val = self.delitem(req[1])
                self.comm.send((4, val), dest=0)
