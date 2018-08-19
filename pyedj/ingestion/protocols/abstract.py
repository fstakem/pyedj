from abc import ABC, abstractmethod


class Abstract(ABC):

    def __init__(self):
        self.stream = None

    @abstractmethod
    def start(self):
        """Start receiving data."""
        return

    @abstractmethod
    def stop(self):
        """Stop receiving data."""
        return
