# -*- coding: utf-8 -*-
import allocator
import sys

if (__name__ == "__main__"):
    if (len(sys.argv) != 3):
        print("Format: {} node_size verbose".format(sys.argv[0]))
        exit(1)

    node_size = int(sys.argv[1])
    verbose   = int(sys.argv[2])
    memory    = allocator.launch(node_size, verbose)

    # Get data from file and set memory by groups of node_size
    f           = open("random.txt", "r")
    memory_size = int(f.readline())
    print(memory_size) # DEBUG
    memory.malloc(memory_size)
    for i in range(memory_size // node_size):
        array_size = min(node_size, memory_size - i * node_size)
        array      = []
        for j in range(array_size):
            array.append(int(f.readline()))
        memory[0, i * node_size: i * node_size + array_size] = array
    f.close()

    # Sort the array
    flag  = False
    shift = node_size // 2
    while (not flag):
        flag = True
        for i in range(2 * memory_size // node_size - 2):
            array        = memory[0, i * shift: i * shift + node_size][0]
            sorted_array = sorted(array)
            if (array != sorted_array):
                flag = False
                memory[0, i * shift: i * shift + node_size] = sorted_array

    # Get the array back and write it to new file
    f           = open("sorted.txt", "w")
    f.write("%d\n" % memory_size)
    for i in range(memory_size // node_size):
        array_size = min(node_size, memory_size - i * node_size)
        array = memory[0, i * node_size: i * node_size + array_size][0]
        for val in array:
            f.write("%d\n" % val)
    f.close()
    memory.close()
