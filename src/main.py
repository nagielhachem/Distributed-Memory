# -*- coding: utf-8 -*-
import allocator
import sys



if (__name__ == "__main__"):
    if (len(sys.argv) != 4):
        print("Format: %s max_size max_child verbose" % sys.argv[0])
        exit(1)

    max_size  = int(sys.argv[1])
    max_child = int(sys.argv[2])
    verbose   = bool(sys.argv[3])

    allocator.launch(max_size, max_child, verbose)

    # TODO: CODE UTILISATEUR
    #  - lire le tableau
    #  - ajouter le tableau
    #  - appliquer la méthode de tri au tableau
    #  - joindre les données des tableaux
    #  - appliquer la méthode etc etc
