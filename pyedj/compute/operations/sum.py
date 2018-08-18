from pyedj.compute.operations.abstract import Abstract


class Sum(Abstract):

    def compute(self, streams):
        data = [v for s in streams]
        data = zip(*data)
        output = [sum(d) for d in data]

        return output