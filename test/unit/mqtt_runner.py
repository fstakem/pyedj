import json
import time
from random import random
from datetime import datetime

from mqtt_client import MqttClient


server = 'localhost'
port = 1883
topic = 'ingestion/test/'

client = MqttClient()
client.connect(server, port, 60)


while True:
    timestamp = datetime.utcnow()
    num = random() * 100
    msg = {
            'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'value': num}
    print(f'Publishing: {msg}')
    client.publish(topic, json.dumps(msg))
    time.sleep(3)
