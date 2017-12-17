# -*- coding: utf-8 -*-
import numpy as np

from mpi4py import MPI



class node():
    def __init__(self, rank, size, nb_child, verbose):
        self.rank     = rank
        self.size     = size
        self.verbose  = verbose

        max_id        = MPI.COMM_WORLD.Get_size()
        self.children = np.array([rank * nb_child - (nb_child - 2) + i for i in range(nb_child)])
        self.children = self.children[np.where(self.children < max_id)]

        print("%d: %s" %(self.rank, self.children)) # DEBUG
        # TODO: voir comment stocker les informations des variables

    def run(self):
        pass
        # TODO: écouter en continu le node supérieur et les fils, et rediriger vers les méthodes associées

    #TODO:
    # - méthode d'ajout de tableaux
    # - méthode de modification de tableaux
    # - méthode de suppression de tableaux
    # - méthode de réorganisation de la mémoire
    # - méthode de traitement de la mémoire

    def __getitem__(key):
        pass

    def __setitem__(key, value):
        pass

    def __delitem__(key, value):
        pass

    def __missing__(non_key):
        return None



def launch(max_size, max_child, verbose=False):
    rank = MPI.COMM_WORLD.Get_rank()

    if (rank == 0):
        return
    else:
        node(rank, max_size, max_child, verbose).run()
