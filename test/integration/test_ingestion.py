import pytest

from datetime import datetime, timedelta
from multiprocessing import Process
from random import random
import json
import time

from pyedj.ingestion.router import Router
from pyedj.compute.stream import Stream

from mqtt_client import MqttClient

# Start mqtt broker
# sudo docker run -it -h mqtt_broker --name mqtt_broker --rm -p 1883:1883 -p 9001:9001 eclipse-mosquitto


def get_msgs(start_time, interval_sec, num):
    msgs = []

    for i in range(num):
        num = random() * 100
        timestamp = start_time + timedelta(seconds=(i*interval_sec))
        msg = {
                'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                'value': num}
        msgs.append(msg)

    return msgs


def send_mqtt_msgs(service_info, msgs, interval_sec):
    client = MqttClient()
    client.connect(service_info['host'], service_info['port'], 60)

    for msg in msgs:
        client.publish(service_info['publish']['default_topic'], json.dumps(msg))
        time.sleep(interval_sec)


def spawn_test_client(service_info, interval_sec, num_msgs):
    msgs = get_msgs(datetime.utcnow(), interval_sec, num_msgs)
    p = Process(target=send_mqtt_msgs, args=(service_info, msgs, interval_sec))
    p.start()
    p.join()


def get_mqtt_info():
    service_info = {}
    service_info['host'] = '172.17.0.2'
    service_info['port'] = 1883
    service_info['protocol'] = 'mqtt'
    service_info['publish'] = {}
    service_info['publish']['default_topic'] = 'test'
    service_info['subscribe'] = {}
    service_info['subscribe']['topics'] = ['test']
    service_info['subscribe']['type'] = 'unblocking'

    return service_info


def get_stream_info():
    stream_info = {}
    stream_info['name'] = 'PirSensor'
    stream_info['debug'] = True
    stream_info['store'] = {}
    stream_info['store']['type'] = 'in_memory'
    stream_info['deserializer'] = {}
    stream_info['deserializer']['type'] = 'json'
    stream_info['enricher'] = {}
    stream_info['enricher']['type'] = 'simple'
    stream_info['schema'] = {}
    stream_info['schema']['fields'] = {}

    stream_info['schema']['fields']['timestamp'] = {
        'type': 'timestamp',
        'default_value': datetime.utcnow(),
        'type_check': True,
        'parser': '%Y-%m-%d %H:%M:%S'
    }

    stream_info['schema']['fields']['value'] = {
        'type': 'float',
        'default_value': 0.0,
        'type_check': True
    }

    return stream_info


def test_simple_storage():
    print()
    service_info = get_mqtt_info()
    stream_info = get_stream_info()
    stream = Stream(stream_info)

    router = Router()
    router.create_route('mqtt_sensor_a', service_info, stream)
    router.start_all()

    spawn_test_client(service_info, 3, 3)
