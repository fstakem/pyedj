from weakref import ref


class Synchronizer(object):

    def __init__(self, name, algorithm, compute_tree):
        self.name = name
        self.algorithm = algorithm
        self.compute_tree = ref(compute_tree)