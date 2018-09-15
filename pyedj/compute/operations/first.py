from pyedj.compute.operations.abstract import Abstract


class First(Abstract):

    def __call__(self, streams):
        return [streams[0][0]]