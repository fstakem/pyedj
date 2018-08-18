from queue import Queue

from paho.mqtt.client import Client

from open_edge.ingestion.protocols.abstract import Abstract


class Mqtt(Abstract):

    def __init__(self):
        self.client = None
        self.service_info = None
        self.queue = Queue()

    def connect(self, service_info):
        self.service_info = service_info
        host = self.service_info['host']
        port = self.service_info['port']

        self.client = Client()
        self.client.on_message = self.on_mqtt_msg
        self.client.connect(host, port, 60)

    def disconnect(self):
        if self.client:
            self.client.disconnect()

    def send_msg(self, msg, tx_info=None):
        topic = None

        if tx_info:
            topic = tx_info['topic']
        elif self.service_info:
            topic = self.service_info['publish']['default_topic']

        if self.client and topic:
            self.client.publish(topic, msg)

    def receive_msgs(self):
        topics = self.service_info['subscribe']['topics']

        if self.client:
            for t in topics:
                self.client.subscribe(t, 0)

            self.client.loop_forever()
        else:
            pass

    def on_msg(self):
        msg = self.queue.get()
        print(f'Received: {msg["topic"]}::{msg["msg"]}')

    def on_mqtt_msg(self, client, userdata, msg):
        payload = {}
        payload['topic'] = msg.topic
        payload['msg'] = msg.payload

        self.queue.put(payload)
        self.on_msg()


Abstract.register(Mqtt)
