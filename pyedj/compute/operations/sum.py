from pyedj.compute.operations.abstract import Abstract


class Sum(Abstract):

    def compute(self, events):
        data = [v for s in events]
        data = zip(*data)
        output = [sum(d) for d in data]

        return output