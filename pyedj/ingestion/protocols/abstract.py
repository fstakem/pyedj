from abc import ABC, abstractmethod


class Abstract(ABC):

    def __init__(self):
        self.stream = None

    @abstractmethod
    def is_connected(self):
        """Get connection status of client."""
        return

    @abstractmethod
    def connect(self, service_info):
        """Connect to the central server."""
        return

    @abstractmethod
    def disconnect(self):
        """Disconnect from central server."""
        return

    @abstractmethod
    def send_msg(self, msg, tx_info=None):
        """Send msg."""
        return

    @abstractmethod
    def receive_msgs(self):
        """Blocking function to start receiving data."""
        return

    @abstractmethod
    def on_msg(self):
        """Callback when msg received."""
        return

    def start(self, stream):
        self.stream = stream

    def stop(self):
        self.stream = None