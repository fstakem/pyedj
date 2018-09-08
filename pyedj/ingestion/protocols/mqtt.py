from queue import Queue

from paho.mqtt.client import Client

from pyedj.ingestion.protocols.abstract import Abstract
from pyedj.compute.event import Event


class MqttError(Exception):
    pass


class Mqtt(Abstract):

    def __init__(self, service_info=None):
        self.connected = False
        self.client = None
        self.queue = Queue()

        super().__init__(service_info)

    def start(self):
        self.connect()
        self.receive_msgs()

    def stop(self):
        self.stop_receiving_msgs()
        self.disconnect()

    def add_stream(self, stream):
        self.streams[stream.name] = stream

    def remove_stream(self, name):
        if name in self.streams.keys():
            self.streams.pop(name)

    def remove_all_streams(self):
        self.streams = {}

    def is_connected(self):
        if self.client and self.connected:
            return True

        return False

    def connect(self):
        if not self.client:
            host = self.service_info['host']
            port = self.service_info['port']

            self.client = Client()
            self.client.on_message = self.on_mqtt_msg
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.connect(host, port, 60)

    def on_connect(self, client, userdata, flags, rc):
        self.connected = True

    def disconnect(self):
        if self.client:
            self.client.disconnect()

    def on_disconnect(self, client, userdata, rc):
        self.client = None
        self.connected = False

    def send_msg(self, msg, tx_info=None):
        if self.client:
            topic = None

            if tx_info:
                topic = tx_info['topic']
            elif self.service_info:
                topic = self.service_info['publish']['default_topic']

            if self.client and topic:
                self.client.publish(topic, msg)
        else:
            raise MqttError('Cannot send message without a client')

    def receive_msgs(self):
        topics = self.service_info['subscribe']['topics']

        if self.client:
            for t in topics:
                self.client.subscribe(t, 0)

            sub_type = self.service_info['subscribe']['type']

            if sub_type == 'blocking':
                self.client.loop_forever()
            elif sub_type == 'unblocking':
                self.client.loop_start()
            elif sub_type == 'polled':
                self.client.loop(self.service_info['subscribe']['loop_time'])
            else:
                self.client.loop(.1)

        else:
            raise MqttError('Cannot receive message without a client')

    def stop_receiving_msgs(self):
        topics = self.service_info['subscribe']['topics']

        if self.client:
            for t in topics:
                self.client.unsubscribe(t)

            self.client.loop_stop()

    def handle_msg(self):
        msg = self.queue.get()
        print(f'Received: {msg["topic"]}::{msg["msg"]}')
        data = super().handle_msg(msg)

        for k, s in self.streams.items():
            events = []

            for d in data:
                timestamp = d.timestamp
                sample = getattr(d, s.handle)
                events.append(Event(timestamp, sample))

            s.handle_msg(events)

    def on_mqtt_msg(self, client, userdata, msg):
        payload = {}
        payload['topic'] = msg.topic
        payload['msg'] = msg.payload

        self.queue.put(payload)
        self.handle_msg()

Abstract.register(Mqtt)
