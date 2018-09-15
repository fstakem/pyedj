from pyedj.compute.operations.abstract import Abstract


class Last(Abstract):

    def __call__(self, stream):
        return [stream[-1]]