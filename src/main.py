# -*- coding: utf-8 -*-
import allocator
import sys

if (__name__ == "__main__"):
    if (len(sys.argv) != 3):
        print("Format: {} max_size verbose".format(sys.argv[0]))
        exit(1)

    max_size  = int(sys.argv[1])
    verbose   = bool(sys.argv[2])

    mem = allocator.launch(max_size, verbose)
    mem.malloc(100)
    mem.malloc(100)
    print(mem[0,   1:8])
    # print(mem[0:2, 3:5])
    mem.close()

    # TODO: CODE UTILISATEUR
    # - lire le tableau
    #  - ajouter le tableau
    #  - appliquer la méthode de tri au tableau
    #  - joindre les données des tableaux
    #  - appliquer la méthode etc etc
