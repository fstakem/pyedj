from paho.mqtt.client import Client


class MqttClient(Client):
    connect_str = {0: 'on_connect: successful',
                   1: 'on_connect: incorrect protocol version',
                   2: 'on_connect: invalid client identifier',
                   3: 'on_connect: server unavailable',
                   4: 'on_connect: bad username or password',
                   5: 'on_connect: not authorised'}
    disconnect_str = {0: 'on_disconnect: called from client',
                      1: 'on_disconnect: unknown caller'}

    def on_connect(self, client, userdata, flags, rc):
        print(MqttClient.connect_str[rc])

    def on_disconnect(self, client, userdata, rc):
        if rc == 0:
            print(MqttClient.disconnect_str[0])
        else:
            print(MqttClient.disconnect_str[1])

    def on_message(self, client, userdata, msg):
        out = f'Received msg topic: {msg.topic} Msg: {msg.payload}'
        print(out)

    def on_publish(self, client, userdata, mid):
        print(f'Published for mid: {mid}')

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f'Subscribed for mid: {mid}')

    def on_unsubscribe(self, client, userdata, mid):
        print(f'Unsubscribed for mid: {mid}')

    def run(self, server, port, topics):
        self.connect(server, port, 60)

        for t in topics:
            mid = self.subscribe(t, 0)
            print(f'Subscribing to {mid}')

        rc = 0

        while rc == 0:
            rc = self.loop()

        return rc
