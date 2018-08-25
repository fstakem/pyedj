from abc import ABC, abstractmethod


class Abstract(ABC):

    def __init__(self, parent):
        self.parent = parent

    @abstractmethod
    def compute(self, events):
        pass